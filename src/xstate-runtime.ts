import { Atom } from "effect/unstable/reactivity";
import type {
  Actor,
  AnyActorLogic,
  ConditionalRequired,
  IsNotNever,
  RequiredActorOptionsKeys,
} from "xstate";
import {
  actorAtom,
  actorRefAtom,
  type ActorAtom,
  type ActorAtomOptions,
} from "./actor-atom";

export type RuntimeActorAtomConfig<TLogic extends AnyActorLogic> = {
  readonly logic: TLogic;
} & ConditionalRequired<
  {
    readonly options?: ActorAtomOptions<TLogic>;
  },
  IsNotNever<RequiredActorOptionsKeys<TLogic>>
>;

export interface XStateRuntime<R, ER> extends Atom.AtomRuntime<R, ER> {
  readonly actorAtom: <TLogic extends AnyActorLogic>(
    config: RuntimeActorAtomConfig<TLogic>
  ) => ActorAtom<TLogic>;
  readonly actorRefAtom: <TLogic extends AnyActorLogic>(
    config: RuntimeActorAtomConfig<TLogic>
  ) => Atom.Atom<Actor<TLogic>>;
}

export const runtime = <R, ER>(
  atomRuntime: Atom.AtomRuntime<R, ER>
): XStateRuntime<R, ER> =>
  Object.assign(atomRuntime, {
    actorAtom: <TLogic extends AnyActorLogic>(
      config: RuntimeActorAtomConfig<TLogic>
    ) => actorAtom({ ...config, runtime: atomRuntime }),
    actorRefAtom: <TLogic extends AnyActorLogic>(
      config: RuntimeActorAtomConfig<TLogic>
    ) => actorRefAtom({ ...config, runtime: atomRuntime }),
  });
