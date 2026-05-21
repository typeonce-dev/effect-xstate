import type { Context } from "effect";
import type { AsyncResult } from "effect/unstable/reactivity";
import type {
  AnyActorLogic,
  AnyActorRef,
  EventObject,
  MachineContext,
  MetaObject,
  ParameterizedObject,
  ProvidedActor,
  StateMachine,
  StateSchema,
  StateValue,
} from "xstate";

export type EffectStopEvent = {
  readonly type: "xstate.stop";
};

export declare const RuntimeRequirementsTypeId: unique symbol;

export interface RuntimeRequirements<R, ER> {
  readonly [RuntimeRequirementsTypeId]: {
    readonly R: (_: never) => R;
    readonly ER: (_: never) => ER;
  };
}

export type NoRuntimeRequirements = RuntimeRequirements<never, never>;

export type RuntimeContextResult<R, ER> = AsyncResult.AsyncResult<
  Context.Context<R>,
  ER
>;

export type RuntimeRequirementOf<TLogic> =
  TLogic extends RuntimeRequirements<infer R, infer ER>
    ? { readonly R: R; readonly ER: ER }
    : TLogic extends StateMachine<
          MachineContext,
          EventObject,
          Record<string, AnyActorRef | undefined>,
          infer TActor,
          ParameterizedObject,
          ParameterizedObject,
          string,
          StateValue,
          string,
          unknown,
          unknown,
          EventObject,
          MetaObject,
          StateSchema
        >
      ? TActor extends { readonly logic: infer TChildLogic }
        ? RuntimeRequirementOf<TChildLogic>
        : { readonly R: never; readonly ER: never }
      : { readonly R: never; readonly ER: never };

export type RequiredServices<TLogic> = RuntimeRequirementOf<TLogic>["R"];

export type RuntimeErrors<TLogic> = RuntimeRequirementOf<TLogic>["ER"];

export type MissingServices<TRequired, TProvided> = Exclude<
  TRequired,
  TProvided
>;

export type RuntimeMismatchMessage<TMissing> = {
  readonly "Missing Effect runtime services": TMissing;
};

export type RuntimeConstraint<TLogic, TProvided> = [MissingServices<
  RequiredServices<TLogic>,
  TProvided
>] extends [never]
  ? object
  : RuntimeMismatchMessage<MissingServices<RequiredServices<TLogic>, TProvided>>;

export type NoRuntimeConstraint<TLogic> = [RequiredServices<TLogic>] extends [
  never,
]
  ? object
  : RuntimeMismatchMessage<RequiredServices<TLogic>>;

export type WithRuntimeRequirements<
  TLogic extends AnyActorLogic,
  R,
  ER,
> = TLogic & RuntimeRequirements<R, ER>;
