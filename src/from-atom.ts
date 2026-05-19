import { Atom, AtomRegistry } from "effect/unstable/reactivity";
import type { ActorLogic, AnyActorRef, AnyActorScope } from "xstate";

export type AtomChangedEvent<A> = {
  readonly type: "atom.changed";
  readonly value: A;
};

export type AtomRefreshEvent = {
  readonly type: "atom.refresh";
};

export type AtomSetEvent<W> = {
  readonly type: "atom.set";
  readonly value: W;
};

export type AtomStopEvent = {
  readonly type: "xstate.stop";
};

export type AtomActorEvent<A, W = never> =
  | AtomRefreshEvent
  | AtomSetEvent<W>
  | AtomStopEvent;

type AtomInternalEvent<A, W> = AtomChangedEvent<A> | AtomActorEvent<A, W>;

export type AtomActorSnapshot<A> =
  | {
      readonly status: "active";
      readonly output: undefined;
      readonly error: undefined;
      readonly context: A;
    }
  | {
      readonly status: "stopped";
      readonly output: undefined;
      readonly error: undefined;
      readonly context: A;
    }
  | {
      readonly status: "error";
      readonly output: undefined;
      readonly error: unknown;
      readonly context: A;
    };

export type FromAtomConfig<A, W = never> = {
  readonly atom: Atom.Atom<A> | Atom.Writable<A, W>;
  readonly registry?: AtomRegistry.AtomRegistry | undefined;
};

const subscriptions = new WeakMap<AnyActorRef, () => void>();

const registryBySystem = new WeakMap<
  AnyActorScope["system"],
  AtomRegistry.AtomRegistry
>();

let currentRegistry: AtomRegistry.AtomRegistry | undefined;

export const registerActorSystemRegistry = (
  system: AnyActorScope["system"],
  registry: AtomRegistry.AtomRegistry
): void => {
  registryBySystem.set(system, registry);
};

export const withActorSystemRegistry = <A>(
  registry: AtomRegistry.AtomRegistry,
  evaluate: () => A
): A => {
  const previous = currentRegistry;
  currentRegistry = registry;
  try {
    return evaluate();
  } finally {
    currentRegistry = previous;
  }
};

const active = <A>(context: A): AtomActorSnapshot<A> => ({
  status: "active",
  output: undefined,
  error: undefined,
  context,
});

const stopped = <A>(context: A): AtomActorSnapshot<A> => ({
  status: "stopped",
  output: undefined,
  error: undefined,
  context,
});

/**
 * Converts an Effect Atom into XState actor logic.
 *
 * Use this when a machine should invoke and react to a value already owned by
 * the Atom graph. When used under `actorAtom`, the invoked actor automatically
 * uses the active `AtomRegistry`, so React, Effect code, and XState observe the
 * same live atom instance without a manual synchronization layer.
 *
 * Writable atoms also accept `atom.set` events, so machines can update Atom
 * state through the actor protocol when that is the desired ownership boundary.
 *
 * @since 0.1.0
 * @category conversions
 * @example
 * const quantityActor = fromAtom({
 *   atom: quantityAtom
 * })
 */
export function fromAtom<A, W = never>(
  config: FromAtomConfig<A, W>
): ActorLogic<AtomActorSnapshot<A>, AtomActorEvent<A, W>, void> {
  let fallbackRegistry: AtomRegistry.AtomRegistry | undefined;
  const getFallbackRegistry = () => {
    fallbackRegistry ??= AtomRegistry.make();
    return fallbackRegistry;
  };
  const getRegistry = (actorScope: AnyActorScope) =>
    config.registry ??
    registryBySystem.get(actorScope.system) ??
    currentRegistry ??
    getFallbackRegistry();

  return {
    transition: (snapshot, event: AtomInternalEvent<A, W>, actorScope) => {
      if (event.type === "atom.changed") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        return active(event.value);
      }
      if (event.type === "atom.refresh") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        const registry = getRegistry(actorScope);
        registry.refresh(config.atom);
        return active(registry.get(config.atom));
      }
      if (event.type === "atom.set" && Atom.isWritable(config.atom)) {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        const registry = getRegistry(actorScope);
        registry.set(config.atom, event.value);
        return active(registry.get(config.atom));
      }
      if (event.type === "xstate.stop") {
        subscriptions.get(actorScope.self)?.();
        subscriptions.delete(actorScope.self);
        return stopped(snapshot.context);
      }
      return snapshot;
    },
    getInitialSnapshot: (actorScope) =>
      active(getRegistry(actorScope).get(config.atom)),
    start: (_snapshot, actorScope) => {
      const registry = getRegistry(actorScope);
      const unsubscribe = registry.subscribe(
        config.atom,
        (value) => {
          (
            actorScope.self as unknown as {
              send: (event: AtomChangedEvent<A>) => void;
            }
          ).send({
            type: "atom.changed",
            value,
          });
        },
        { immediate: true }
      );
      subscriptions.set(actorScope.self, unsubscribe);
    },
    getPersistedSnapshot: (snapshot) => snapshot,
  };
}
