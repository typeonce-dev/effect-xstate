import { Atom, AtomRegistry } from "effect/unstable/reactivity";
import type {
  Actor,
  ActorOptions,
  AnyActorLogic,
  ConditionalRequired,
  InputFrom,
  IsNotNever,
  RequiredActorOptionsKeys,
} from "xstate";
import { createActor } from "xstate";
import {
  actorAtom,
  actorRefAtom,
  type ActorAtomRuntimeConfig,
  type ActorAtomOptions,
  type RuntimeActorAtom,
} from "./atoms";
import { registerActorSystemRuntimeContext } from "./runtime-context";
import type { RuntimeConstraint } from "./types";

export type RuntimeActorAtomConfig<
  TLogic extends AnyActorLogic,
  R = never,
> = {
  readonly logic: TLogic;
} & RuntimeConstraint<TLogic, R> &
  ConditionalRequired<
  {
    readonly options?: ActorAtomOptions<TLogic>;
  },
  IsNotNever<RequiredActorOptionsKeys<TLogic>>
>;

export type XStateRuntimeActorConfig<
  TLogic extends AnyActorLogic,
  R,
> = {
  readonly logic: TLogic;
} & RuntimeConstraint<TLogic, R> &
  ConditionalRequired<
    {
      readonly options?: ActorOptions<TLogic> & {
        readonly [K in RequiredActorOptionsKeys<TLogic>]: InputFrom<TLogic>;
      };
    },
    IsNotNever<RequiredActorOptionsKeys<TLogic>>
  >;

export interface XStateRuntime<R, ER> extends Atom.AtomRuntime<R, ER> {
  readonly actorAtom: <TLogic extends AnyActorLogic>(
    config: {
      readonly logic: TLogic;
    } & RuntimeConstraint<TLogic, R> &
      ConditionalRequired<
        {
          readonly options?: ActorAtomOptions<TLogic>;
        },
        IsNotNever<RequiredActorOptionsKeys<TLogic>>
      >
  ) => RuntimeActorAtom<TLogic, ER>;
  readonly actorRefAtom: <TLogic extends AnyActorLogic>(
    config: {
      readonly logic: TLogic;
    } & RuntimeConstraint<TLogic, R> &
      ConditionalRequired<
        {
          readonly options?: ActorAtomOptions<TLogic>;
        },
        IsNotNever<RequiredActorOptionsKeys<TLogic>>
      >
  ) => Atom.Atom<Actor<TLogic>>;
  readonly createActor: <TLogic extends AnyActorLogic>(
    config: XStateRuntimeActorConfig<TLogic, R>
  ) => Actor<TLogic>;
}

export const runtime = <R, ER>(
  atomRuntime: Atom.AtomRuntime<R, ER>
): XStateRuntime<R, ER> =>
  Object.assign(atomRuntime, {
    actorAtom: <TLogic extends AnyActorLogic>(
      config: {
        readonly logic: TLogic;
        readonly options?: ActorAtomOptions<TLogic>;
      }
    ) =>
      actorAtom({
        ...config,
        runtime: atomRuntime,
      } as ActorAtomRuntimeConfig<TLogic, R, ER>) as RuntimeActorAtom<TLogic, ER>,
    actorRefAtom: <TLogic extends AnyActorLogic>(
      config: {
        readonly logic: TLogic;
        readonly options?: ActorAtomOptions<TLogic>;
      }
    ) =>
      actorRefAtom({
        ...config,
        runtime: atomRuntime,
      } as ActorAtomRuntimeConfig<TLogic, R, ER>) as Atom.Atom<Actor<TLogic>>,
    createActor: <TLogic extends AnyActorLogic>(
      config: XStateRuntimeActorConfig<TLogic, R>
    ) => {
      const registry = AtomRegistry.make();
      const unmount = registry.mount(atomRuntime);
      const actor = createActor(config.logic, config.options);
      const unregister = registerActorSystemRuntimeContext(actor.system, {
        get: () => registry.get(atomRuntime) as any,
        subscribe: (onChange) => registry.subscribe(atomRuntime, onChange),
      });
      let closed = false;
      const close = () => {
        if (closed) {
          return;
        }
        closed = true;
        unregister();
        unmount();
      };
      actor.subscribe({
        complete: close,
        error: close,
      });
      const stop = actor.stop.bind(actor);
      actor.stop = () => {
        stop();
        close();
        return actor;
      };
      return actor;
    },
  });
