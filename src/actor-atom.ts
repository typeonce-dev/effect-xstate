import { Option } from "effect";
import { Atom } from "effect/unstable/reactivity";
import {
  createActor,
  type Actor,
  type ActorOptions,
  type AnyActorLogic,
  type ConditionalRequired,
  type EmittedFrom,
  type EventFromLogic,
  type InputFrom,
  type IsNotNever,
  type RequiredActorOptionsKeys,
  type Snapshot,
  type SnapshotFrom,
} from "xstate";

export type ActorAtomOptions<TLogic extends AnyActorLogic> =
  ActorOptions<TLogic> & {
    readonly [K in RequiredActorOptionsKeys<TLogic>]: InputFrom<TLogic>;
  };

export type ActorAtomConfig<TLogic extends AnyActorLogic> = {
  readonly logic: TLogic;
} & ConditionalRequired<
  {
    readonly options?: ActorAtomOptions<TLogic>;
  },
  IsNotNever<RequiredActorOptionsKeys<TLogic>>
>;

export interface ActorAtom<TLogic extends AnyActorLogic> extends Atom.Writable<
  SnapshotFrom<TLogic>,
  EventFromLogic<TLogic>
> {
  readonly actor: Atom.Atom<Actor<TLogic>>;
}

export type EmittedSelection<
  TLogic extends AnyActorLogic,
  TType extends EmittedFrom<TLogic>["type"] | "*",
> = EmittedFrom<TLogic> &
  (TType extends "*" ? object : { readonly type: TType });

/**
 * Creates an Atom-owned XState actor reference.
 *
 * The actor is started lazily by the active Atom registry and stopped by the
 * Atom finalizer. Use this when the live `Actor` itself must be available to
 * other atoms, inspection tools, or lower-level integrations.
 *
 * @since 0.1.0
 * @category constructors
 * @example
 * const checkoutRef = actorRefAtom({ logic: checkoutMachine })
 */
export const actorRefAtom = <TLogic extends AnyActorLogic>(
  config: ActorAtomConfig<TLogic>
): Atom.Atom<Actor<TLogic>> =>
  Atom.make((get) => {
    const actor = createActor(config.logic, config.options);
    actor.start();
    get.addFinalizer(() => {
      actor.stop();
    });
    return actor;
  });

/**
 * Wraps XState actor logic as a writable Effect Atom.
 *
 * Reading the atom returns the current XState snapshot. Writing to the atom
 * sends an event to the actor. This makes the machine a first-class node in the
 * Atom graph while XState still owns states, transitions, children, invokes,
 * emitted events, and persistence.
 *
 * @since 0.1.0
 * @category constructors
 * @example
 * const checkoutActor = actorAtom({ logic: checkoutMachine })
 */
export const actorAtom = <TLogic extends AnyActorLogic>(
  config: ActorAtomConfig<TLogic>
): ActorAtom<TLogic> => {
  const actor = actorRefAtom(config);
  const snapshot = Atom.writable<SnapshotFrom<TLogic>, EventFromLogic<TLogic>>(
    (get) => {
      const actorRef = get(actor);
      const subscription = actorRef.subscribe({
        next: (value) => {
          get.setSelf(value);
        },
        error: () => {
          get.setSelf(actorRef.getSnapshot());
        },
      });
      get.addFinalizer(() => {
        subscription.unsubscribe();
      });
      return actorRef.getSnapshot();
    },
    (ctx, event) => {
      ctx.get(actor).send(event);
    }
  );
  return Object.assign(snapshot, { actor });
};

/**
 * Creates an Atom projection over an `actorAtom` snapshot.
 *
 * Use this instead of selecting machine state inside React components when the
 * selected value is part of the application state graph. The selector is typed
 * from the XState logic and the atom updates only when the selected value
 * changes according to `equal`.
 *
 * @since 0.1.0
 * @category combinators
 * @example
 * const canSubmitAtom = selectAtom({
 *   actor: checkoutActor,
 *   selector: (snapshot) => snapshot.matches("ready")
 * })
 */
export const selectAtom = <TLogic extends AnyActorLogic, TSelected>(config: {
  readonly actor: ActorAtom<TLogic>;
  readonly selector: (snapshot: SnapshotFrom<TLogic>) => TSelected;
  readonly equal?: ((left: TSelected, right: TSelected) => boolean) | undefined;
}): Atom.Atom<TSelected> =>
  Atom.readable((get) => {
    const actorRef = get(config.actor.actor);
    let previous = config.selector(actorRef.getSnapshot());
    const equal = config.equal ?? Object.is;
    const subscription = actorRef.subscribe({
      next: (snapshot) => {
        const next = config.selector(snapshot);
        if (!equal(previous, next)) {
          previous = next;
          get.setSelf(next);
        }
      },
    });
    get.addFinalizer(() => {
      subscription.unsubscribe();
    });
    return previous;
  });

/**
 * Exposes XState emitted events as an Atom.
 *
 * This is useful for side channels that should not be modeled as durable
 * machine context, such as telemetry, completion notifications, or domain
 * events consumed elsewhere in the Atom graph.
 *
 * @since 0.1.0
 * @category combinators
 * @example
 * const completedAtom = emittedAtom({
 *   actor: checkoutActor,
 *   type: "checkout.completed"
 * })
 */
export const emittedAtom = <
  TLogic extends AnyActorLogic,
  TType extends EmittedFrom<TLogic>["type"] | "*",
>(config: {
  readonly actor: ActorAtom<TLogic>;
  readonly type: TType;
}): Atom.Atom<Option.Option<EmittedSelection<TLogic, TType>>> =>
  Atom.readable((get) => {
    const actorRef = get(config.actor.actor);
    const subscription = actorRef.on(config.type, (event) => {
      get.setSelf(Option.some(event));
    });
    get.addFinalizer(() => {
      subscription.unsubscribe();
    });
    return Option.none();
  });

/**
 * Exposes an actor's persisted XState snapshot as an Atom.
 *
 * Use this when persistence, debugging, or hydration code should observe the
 * same lazy actor lifecycle as the rest of the Atom graph.
 *
 * @since 0.1.0
 * @category combinators
 * @example
 * const snapshotAtom = persistedAtom({ actor: checkoutActor })
 */
export const persistedAtom = <TLogic extends AnyActorLogic>(config: {
  readonly actor: ActorAtom<TLogic>;
}): Atom.Atom<Snapshot<unknown>> =>
  Atom.readable((get) => {
    const actorRef = get(config.actor.actor);
    const update = () => {
      get.setSelf(actorRef.getPersistedSnapshot());
    };
    const subscription = actorRef.subscribe({
      next: update,
      error: update,
      complete: update,
    });
    get.addFinalizer(() => {
      subscription.unsubscribe();
    });
    return actorRef.getPersistedSnapshot();
  });
