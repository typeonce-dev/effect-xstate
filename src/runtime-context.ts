import { Cause, Context, Exit } from "effect";
import type {
  ActorScope,
  ActorSystem,
  ActorSystemInfo,
  AnyActorScope,
  EventObject,
  Snapshot,
} from "xstate";
import type { RuntimeContextResult } from "./types.ts";

type RuntimeContextBridge<R = unknown, ER = unknown> = {
  readonly get: () => RuntimeContextResult<R, ER>;
  readonly subscribe: (onChange: () => void) => () => void;
};

const runtimeContextBySystem = new WeakMap<
  AnyActorScope["system"],
  RuntimeContextBridge
>();

export const registerActorSystemRuntimeContext = <R, ER>(
  system: AnyActorScope["system"],
  bridge: RuntimeContextBridge<R, ER>
): (() => void) => {
  const erasedBridge = bridge as unknown as RuntimeContextBridge;
  runtimeContextBySystem.set(system, erasedBridge);
  return () => {
    if (runtimeContextBySystem.get(system) === erasedBridge) {
      runtimeContextBySystem.delete(system);
    }
  };
};

export const getActorSystemRuntimeResult = (
  system: AnyActorScope["system"]
): RuntimeContextResult<unknown, unknown> | undefined =>
  runtimeContextBySystem.get(system)?.get();

export const subscribeActorSystemRuntime = (
  system: AnyActorScope["system"],
  onChange: () => void
): (() => void) | undefined => runtimeContextBySystem.get(system)?.subscribe(onChange);

type RuntimeReadinessScope<
  TSnapshot extends Snapshot<unknown>,
  TEvent extends EventObject,
  TEmitted extends EventObject,
> = ActorScope<TSnapshot, TEvent, ActorSystem<ActorSystemInfo>, TEmitted>;

export const waitForActorSystemRuntime = <
  R,
  TSnapshot extends Snapshot<unknown>,
  TEvent extends EventObject,
  TEmitted extends EventObject,
>(options: {
  readonly actorScope: RuntimeReadinessScope<TSnapshot, TEvent, TEmitted>;
  readonly runtimeSubscriptions: WeakMap<object, () => void>;
  readonly runtimeResolvedActors: WeakSet<object>;
  readonly start: (services: Context.Context<R>) => void;
  readonly fail: (cause: Cause.Cause<unknown>) => void;
}): void => {
  const resolve = (evaluate: () => void) => {
    if (
      options.runtimeResolvedActors.has(options.actorScope.self) ||
      options.actorScope.self.getSnapshot().status !== "active"
    ) {
      return;
    }
    options.runtimeResolvedActors.add(options.actorScope.self);
    options.runtimeSubscriptions.get(options.actorScope.self)?.();
    options.runtimeSubscriptions.delete(options.actorScope.self);
    evaluate();
  };
  const startWhenRuntimeReady = () => {
    const result = getActorSystemRuntimeResult(options.actorScope.system);
    if (result === undefined) {
      resolve(() => {
        options.start(Context.empty() as Context.Context<R>);
      });
      return;
    }
    if (result._tag === "Success") {
      resolve(() => {
        options.start(result.value as Context.Context<R>);
      });
      return;
    }
    if (result._tag === "Failure") {
      resolve(() => {
        options.fail(result.cause);
      });
    }
  };

  startWhenRuntimeReady();
  if (getActorSystemRuntimeResult(options.actorScope.system)?._tag === "Initial") {
    const unsubscribe = subscribeActorSystemRuntime(
      options.actorScope.system,
      () => {
        startWhenRuntimeReady();
      }
    );
    if (unsubscribe !== undefined) {
      if (
        options.runtimeResolvedActors.has(options.actorScope.self) ||
        options.actorScope.self.getSnapshot().status !== "active"
      ) {
        unsubscribe();
      } else {
        options.runtimeSubscriptions.set(options.actorScope.self, unsubscribe);
        startWhenRuntimeReady();
      }
    }
  }
};

export const getActorSystemRuntimeContextExit = (
  system: AnyActorScope["system"]
): Exit.Exit<Context.Context<unknown>, unknown> => {
  const result = getActorSystemRuntimeResult(system);
  if (result === undefined) {
    return Exit.succeed(Context.empty() as Context.Context<unknown>);
  }
  switch (result._tag) {
    case "Success":
      return Exit.succeed(result.value);
    case "Failure":
      return Exit.failCause(result.cause);
    case "Initial":
      return Exit.die(
        new Cause.NoSuchElementError("Atom runtime is not available yet")
      );
  }
};
