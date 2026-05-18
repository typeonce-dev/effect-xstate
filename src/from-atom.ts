import { Atom, AtomRegistry } from "effect/unstable/reactivity";
import type { ActorLogic, AnyActorRef } from "xstate";

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
  | AtomChangedEvent<A>
  | AtomRefreshEvent
  | AtomSetEvent<W>
  | AtomStopEvent;

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
 * the Atom graph. Sharing the same `AtomRegistry` lets React, Effect code, and
 * the invoked XState actor observe the same live atom instance without a manual
 * synchronization layer.
 *
 * Writable atoms also accept `atom.set` events, so machines can update Atom
 * state through the actor protocol when that is the desired ownership boundary.
 *
 * @since 0.1.0
 * @category conversions
 * @example
 * const quantityActor = fromAtom({
 *   atom: quantityAtom,
 *   registry
 * })
 */
export function fromAtom<A, W = never>(
  config: FromAtomConfig<A, W>
): ActorLogic<AtomActorSnapshot<A>, AtomActorEvent<A, W>, void> {
  const registry = config.registry ?? AtomRegistry.make();
  return {
    transition: (snapshot, event, actorScope) => {
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
        registry.refresh(config.atom);
        return active(registry.get(config.atom));
      }
      if (event.type === "atom.set" && Atom.isWritable(config.atom)) {
        if (snapshot.status !== "active") {
          return snapshot;
        }
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
    getInitialSnapshot: () => active(registry.get(config.atom)),
    start: (_snapshot, actorScope) => {
      const unsubscribe = registry.subscribe(
        config.atom,
        (value) => {
          actorScope.self.send({
            type: "atom.changed",
            value,
          });
        },
        { immediate: false }
      );
      subscriptions.set(actorScope.self, unsubscribe);
    },
    getPersistedSnapshot: (snapshot) => snapshot,
  };
}
