import { Context, Exit } from "effect";
import type { AnyActorScope } from "xstate";

const runtimeContextBySystem = new WeakMap<
  AnyActorScope["system"],
  () => Exit.Exit<Context.Context<unknown>, unknown>
>();

export const registerActorSystemRuntimeContext = (
  system: AnyActorScope["system"],
  getContext: () => Exit.Exit<Context.Context<unknown>, unknown>
): void => {
  runtimeContextBySystem.set(system, getContext);
};

export const getActorSystemRuntimeContext = (
  system: AnyActorScope["system"]
): Exit.Exit<Context.Context<unknown>, unknown> =>
  runtimeContextBySystem.get(system)?.() ??
  Exit.succeed(Context.empty() as Context.Context<unknown>);
