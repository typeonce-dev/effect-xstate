import { Cause, Context, Effect, Exit, Fiber, Option, Stream } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";
import type {
  ActorLogic,
  ActorScope,
  ActorSystem,
  ActorSystemInfo,
  AnyEventObject,
  EventObject,
  Snapshot,
} from "xstate";
import {
  getActorSystemRuntimeResult,
  subscribeActorSystemRuntime,
} from "./actor-system-runtime";
import type { WithRuntimeRequirements } from "./internal";

export type EffectSuccessEvent<A> = {
  readonly type: "effect.success";
  readonly value: A;
};

export type EffectFailureEvent<E> = {
  readonly type: "effect.failure";
  readonly cause: Cause.Cause<E>;
};

export type EffectStopEvent = {
  readonly type: "xstate.stop";
};

export type EffectActorEvent<A, E> = EffectStopEvent;

type EffectInternalEvent<A, E> =
  | EffectSuccessEvent<A>
  | EffectFailureEvent<E>
  | EffectStopEvent;

export type EffectActorSnapshot<A, E, TInput> =
  | {
      readonly status: "active";
      readonly output: undefined;
      readonly error: undefined;
      readonly input: TInput;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "done";
      readonly output: A;
      readonly error: undefined;
      readonly input: undefined;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "error";
      readonly output: undefined;
      readonly error: Cause.Cause<E>;
      readonly cause: Cause.Cause<E>;
      readonly input: undefined;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "stopped";
      readonly output: undefined;
      readonly error: undefined;
      readonly input: undefined;
      readonly result: AsyncResult.AsyncResult<A, E>;
    };

export type FromEffectConfig<
  A,
  E,
  TInput,
  TEmitted extends EventObject,
  R,
> = {
  readonly effect: (scope: {
    readonly input: TInput;
    readonly emit: (event: TEmitted) => void;
  }) => Effect.Effect<A, E, R>;
};

const active = <A, E, TInput>(
  input: TInput
): EffectActorSnapshot<A, E, TInput> => ({
  status: "active",
  output: undefined,
  error: undefined,
  input,
  result: AsyncResult.initial(true),
});

const done = <A, E, TInput>(value: A): EffectActorSnapshot<A, E, TInput> => ({
  status: "done",
  output: value,
  error: undefined,
  input: undefined,
  result: AsyncResult.success(value),
});

const failed = <A, E, TInput>(
  cause: Cause.Cause<E>
): EffectActorSnapshot<A, E, TInput> => ({
  status: "error",
  output: undefined,
  error: cause,
  cause,
  input: undefined,
  result: AsyncResult.failure(cause),
});

const stopped = <A, E, TInput>(
  result: AsyncResult.AsyncResult<A, E>
): EffectActorSnapshot<A, E, TInput> => ({
  status: "stopped",
  output: undefined,
  error: undefined,
  input: undefined,
  result,
});

const settleResult = <A, E>(
  result: AsyncResult.AsyncResult<A, E>
): AsyncResult.AsyncResult<A, E> => {
  switch (result._tag) {
    case "Initial":
      return AsyncResult.initial(false);
    case "Success":
      return AsyncResult.success(result.value, {
        timestamp: result.timestamp,
      });
    case "Failure":
      return AsyncResult.failure(result.cause, {
        previousSuccess: result.previousSuccess,
      });
  }
};

const relayIfActive = <
  TSnapshot extends Snapshot<unknown>,
  TEvent extends EventObject,
  TEmitted extends EventObject,
>(
  actorScope: ActorScope<
    TSnapshot,
    TEvent,
    ActorSystem<ActorSystemInfo>,
    TEmitted
  >,
  event: AnyEventObject
): void => {
  if (actorScope.self.getSnapshot().status !== "active") {
    return;
  }
  (actorScope.self as unknown as { send: (event: AnyEventObject) => void }).send(
    event
  );
};

/**
 * Converts an Effect workflow into XState actor logic.
 *
 * Use this for invoked actors where XState should own the state-machine
 * lifecycle and input/output protocol, while Effect owns business logic,
 * failures, fibers, interruption, and optional emitted events.
 *
 * @since 0.1.0
 * @category conversions
 * @example
 * const pricing = fromEffect({
 *   effect: ({ input }) => Effect.succeed(input.quantity * 12)
 * })
 */
export const isFailureSnapshot = <E>(
  snapshot: Snapshot<unknown>
): snapshot is Snapshot<unknown> & {
  readonly status: "error";
  readonly cause?: Cause.Cause<E>;
  readonly error: Cause.Cause<E>;
} => snapshot.status === "error";

export const failureCause = <E>(
  snapshot: Snapshot<unknown> & {
    readonly status: "error";
    readonly cause?: Cause.Cause<E>;
    readonly error: Cause.Cause<E>;
  }
): Cause.Cause<E> => snapshot.cause ?? snapshot.error;

export const failureValue = <E>(
  snapshot: Snapshot<unknown> & {
    readonly status: "error";
    readonly cause?: Cause.Cause<E>;
    readonly error: Cause.Cause<E>;
  }
): E | undefined => Cause.findErrorOption(failureCause(snapshot)).pipe(
  Option.getOrUndefined
);

export const prettyCause = <E>(cause: Cause.Cause<E>): string =>
  Cause.pretty(cause);

export const fromEffect = <
  A,
  E = never,
  TInput = void,
  TEmitted extends EventObject = EventObject,
  R = never,
>(
  config: FromEffectConfig<A, E, TInput, TEmitted, R>
): WithRuntimeRequirements<
  ActorLogic<
  EffectActorSnapshot<A, E, TInput>,
  EffectActorEvent<A, E>,
  TInput,
  ActorSystem<ActorSystemInfo>,
  TEmitted
  >,
  R,
  never
> => {
  const fibers = new WeakMap<object, Fiber.Fiber<unknown, unknown>>();
  const runtimeSubscriptions = new WeakMap<object, () => void>();
  const logic: ActorLogic<
    EffectActorSnapshot<A, E, TInput>,
    EffectActorEvent<A, E>,
    TInput,
    ActorSystem<ActorSystemInfo>,
    TEmitted
  > = {
    transition: (
      snapshot: EffectActorSnapshot<A, E, TInput>,
      event: EffectInternalEvent<A, E>,
      actorScope
    ) => {
      if (event.type === "effect.success") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        return done(event.value);
      }
      if (event.type === "effect.failure") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        return failed(event.cause);
      }
      if (event.type === "xstate.stop") {
        fibers.get(actorScope.self)?.interruptUnsafe();
        fibers.delete(actorScope.self);
        runtimeSubscriptions.get(actorScope.self)?.();
        runtimeSubscriptions.delete(actorScope.self);
        return stopped(settleResult(snapshot.result));
      }
      return snapshot;
    },
    getInitialSnapshot: (_actorScope, input) => active(input),
    start: (snapshot, actorScope) => {
      if (snapshot.status !== "active") {
        return;
      }
      const startFiber = (services: Context.Context<R>) => {
        runtimeSubscriptions.get(actorScope.self)?.();
        runtimeSubscriptions.delete(actorScope.self);
        const fiber = Effect.runForkWith(services)(
          Effect.suspend(() =>
            config.effect({
              input: snapshot.input,
              emit: actorScope.emit,
            })
          )
        );
        fibers.set(actorScope.self, fiber);
        fiber.addObserver((exit) => {
          fibers.delete(actorScope.self);
          if (Exit.isSuccess(exit)) {
            relayIfActive(actorScope, {
              type: "effect.success",
              value: exit.value,
            });
          } else {
            relayIfActive(actorScope, {
              type: "effect.failure",
              cause: exit.cause,
            });
          }
        });
      };
      const startWhenRuntimeReady = () => {
        const result = getActorSystemRuntimeResult(actorScope.system);
        if (result === undefined) {
          startFiber(Context.empty() as Context.Context<R>);
          return;
        }
        if (result._tag === "Success") {
          startFiber(result.value as Context.Context<R>);
          return;
        }
        if (result._tag === "Failure") {
          runtimeSubscriptions.get(actorScope.self)?.();
          runtimeSubscriptions.delete(actorScope.self);
          relayIfActive(actorScope, {
            type: "effect.failure",
            cause: result.cause as Cause.Cause<E>,
          });
        }
      };
      startWhenRuntimeReady();
      if (getActorSystemRuntimeResult(actorScope.system)?._tag === "Initial") {
        const unsubscribe = subscribeActorSystemRuntime(actorScope.system, () => {
          startWhenRuntimeReady();
        });
        if (unsubscribe !== undefined) {
          runtimeSubscriptions.set(actorScope.self, unsubscribe);
        }
      }
    },
    getPersistedSnapshot: (snapshot) => snapshot,
  };
  return logic as WithRuntimeRequirements<typeof logic, R, never>;
};

export type StreamNextEvent<A> = {
  readonly type: "stream.next";
  readonly value: A;
};

export type StreamDoneEvent = {
  readonly type: "stream.done";
};

export type StreamFailureEvent<E> = {
  readonly type: "stream.failure";
  readonly cause: Cause.Cause<E>;
};

export type StreamActorEvent<A, E> = EffectStopEvent;

type StreamInternalEvent<A, E> =
  | StreamNextEvent<A>
  | StreamDoneEvent
  | StreamFailureEvent<E>
  | EffectStopEvent;

export type StreamActorSnapshot<A, E, TInput, TAccum = ReadonlyArray<A>> =
  | {
      readonly status: "active";
      readonly output: undefined;
      readonly error: undefined;
      readonly cause: undefined;
      readonly input: TInput;
      readonly value: TAccum;
      readonly latest: A | undefined;
      readonly count: number;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "done";
      readonly output: TAccum;
      readonly error: undefined;
      readonly cause: undefined;
      readonly input: undefined;
      readonly value: TAccum;
      readonly latest: A | undefined;
      readonly count: number;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "error";
      readonly output: undefined;
      readonly error: Cause.Cause<E>;
      readonly cause: Cause.Cause<E>;
      readonly input: undefined;
      readonly value: TAccum;
      readonly latest: A | undefined;
      readonly count: number;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "stopped";
      readonly output: undefined;
      readonly error: undefined;
      readonly cause: undefined;
      readonly input: undefined;
      readonly value: TAccum;
      readonly latest: A | undefined;
      readonly count: number;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    };

export type StreamAccumulationPolicy<A, TAccum = ReadonlyArray<A>> =
  | {
      readonly mode?: "collect";
      readonly maxItems?: number | undefined;
    }
  | {
      readonly mode: "latest";
    }
  | {
      readonly mode: "none";
    }
  | {
      readonly mode: "reduce";
      readonly seed: TAccum;
      readonly reducer: (accumulator: TAccum, value: A) => TAccum;
    };

export type FromStreamConfig<
  A,
  E,
  TInput,
  TEmitted extends EventObject,
  R,
  TAccum,
> = {
  readonly stream: (scope: {
    readonly input: TInput;
    readonly emit: (event: TEmitted) => void;
  }) => Stream.Stream<A, E, R>;
  readonly accumulation?: StreamAccumulationPolicy<A, TAccum> | undefined;
};

const applyStreamPolicy = <A, TAccum>(
  policy: StreamAccumulationPolicy<A, TAccum> | undefined,
  items: ReadonlyArray<A>,
  accumulator: TAccum,
  value: A
): { readonly items: ReadonlyArray<A>; readonly value: TAccum } => {
  switch (policy?.mode) {
    case "latest":
      return { items: [value], value: [value] as TAccum };
    case "none":
      return { items: [], value: [] as TAccum };
    case "reduce":
      return {
        items,
        value: policy.reducer(accumulator, value),
      };
    case "collect":
    case undefined: {
      const next = [...items, value];
      const collected = policy?.maxItems === undefined
        ? next
        : next.slice(-Math.max(0, policy.maxItems));
      return { items: collected, value: collected as TAccum };
    }
  }
};

const streamActive = <A, E, TInput, TAccum>(
  input: TInput,
  accumulation: StreamAccumulationPolicy<A, TAccum> | undefined
): StreamActorSnapshot<A, E, TInput, TAccum> => ({
  status: "active",
  output: undefined,
  error: undefined,
  cause: undefined,
  input,
  value:
    accumulation?.mode === "none"
      ? ([] as TAccum)
      : accumulation?.mode === "reduce"
        ? accumulation.seed
        : ([] as TAccum),
  latest: undefined,
  count: 0,
  items: [],
  result: AsyncResult.initial(true),
});

/**
 * Converts an Effect Stream into XState actor logic.
 *
 * Use this when a machine invokes a streaming Effect workflow and needs each
 * emitted value reflected in the actor snapshot. Stopping the XState actor
 * interrupts the running Effect fiber.
 *
 * @since 0.1.0
 * @category conversions
 * @example
 * const prices = fromStream({
 *   stream: ({ input }) => Stream.fromIterable(input.values)
 * })
 */
export const fromStream = <
  A,
  E = never,
  TInput = void,
  TEmitted extends EventObject = EventObject,
  R = never,
  TAccum = ReadonlyArray<A>,
>(
  config: FromStreamConfig<A, E, TInput, TEmitted, R, TAccum>
): WithRuntimeRequirements<
  ActorLogic<
    StreamActorSnapshot<A, E, TInput, TAccum>,
    StreamActorEvent<A, E>,
    TInput,
    ActorSystem<ActorSystemInfo>,
    TEmitted
  >,
  R,
  never
> => {
  const fibers = new WeakMap<object, Fiber.Fiber<unknown, unknown>>();
  const runtimeSubscriptions = new WeakMap<object, () => void>();
  const logic: ActorLogic<
    StreamActorSnapshot<A, E, TInput, TAccum>,
    StreamActorEvent<A, E>,
    TInput,
    ActorSystem<ActorSystemInfo>,
    TEmitted
  > = {
    transition: (
      snapshot: StreamActorSnapshot<A, E, TInput, TAccum>,
      event: StreamInternalEvent<A, E>,
      actorScope
    ) => {
      if (event.type === "stream.next") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        const next = applyStreamPolicy(
          config.accumulation,
          snapshot.items,
          snapshot.value,
          event.value
        );
        return {
          status: "active",
          output: undefined,
          error: undefined,
          cause: undefined,
          input: snapshot.input,
          value: next.value,
          latest: event.value,
          count: snapshot.count + 1,
          items: next.items,
          result: AsyncResult.success(event.value, { waiting: true }),
        };
      }
      if (event.type === "stream.done") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        return {
          status: "done",
          output: snapshot.value,
          error: undefined,
          cause: undefined,
          input: undefined,
          value: snapshot.value,
          latest: snapshot.latest,
          count: snapshot.count,
          items: snapshot.items,
          result: settleResult(snapshot.result),
        };
      }
      if (event.type === "stream.failure") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        return {
          status: "error",
          output: undefined,
          error: event.cause,
          cause: event.cause,
          input: undefined,
          value: snapshot.value,
          latest: snapshot.latest,
          count: snapshot.count,
          items: snapshot.items,
          result: AsyncResult.failure(event.cause),
        };
      }
      if (event.type === "xstate.stop") {
        fibers.get(actorScope.self)?.interruptUnsafe();
        fibers.delete(actorScope.self);
        runtimeSubscriptions.get(actorScope.self)?.();
        runtimeSubscriptions.delete(actorScope.self);
        return {
          status: "stopped",
          output: undefined,
          error: undefined,
          cause: undefined,
          input: undefined,
          value: snapshot.value,
          latest: snapshot.latest,
          count: snapshot.count,
          items: snapshot.items,
          result: settleResult(snapshot.result),
        };
      }
      return snapshot;
    },
    getInitialSnapshot: (_actorScope, input) =>
      streamActive(input, config.accumulation),
    start: (snapshot, actorScope) => {
      if (snapshot.status !== "active") {
        return;
      }
      const startFiber = (services: Context.Context<R>) => {
        runtimeSubscriptions.get(actorScope.self)?.();
        runtimeSubscriptions.delete(actorScope.self);
        const fiber = Effect.runForkWith(services)(
          Stream.suspend(() =>
            config.stream({
              input: snapshot.input,
              emit: actorScope.emit,
            })
          ).pipe(
              Stream.runForEach((value) =>
                Effect.sync(() => {
                  relayIfActive(actorScope, {
                    type: "stream.next",
                    value,
                  });
                })
              )
            )
        );
        fibers.set(actorScope.self, fiber);
        fiber.addObserver((exit) => {
          fibers.delete(actorScope.self);
          if (Exit.isSuccess(exit)) {
            relayIfActive(actorScope, {
              type: "stream.done",
            });
          } else {
            relayIfActive(actorScope, {
              type: "stream.failure",
              cause: exit.cause,
            });
          }
        });
      };
      const startWhenRuntimeReady = () => {
        const result = getActorSystemRuntimeResult(actorScope.system);
        if (result === undefined) {
          startFiber(Context.empty() as Context.Context<R>);
          return;
        }
        if (result._tag === "Success") {
          startFiber(result.value as Context.Context<R>);
          return;
        }
        if (result._tag === "Failure") {
          runtimeSubscriptions.get(actorScope.self)?.();
          runtimeSubscriptions.delete(actorScope.self);
          relayIfActive(actorScope, {
            type: "stream.failure",
            cause: result.cause as Cause.Cause<E>,
          });
        }
      };
      startWhenRuntimeReady();
      if (getActorSystemRuntimeResult(actorScope.system)?._tag === "Initial") {
        const unsubscribe = subscribeActorSystemRuntime(actorScope.system, () => {
          startWhenRuntimeReady();
        });
        if (unsubscribe !== undefined) {
          runtimeSubscriptions.set(actorScope.self, unsubscribe);
        }
      }
    },
    getPersistedSnapshot: (snapshot) => snapshot,
  };
  return logic as WithRuntimeRequirements<typeof logic, R, never>;
};
