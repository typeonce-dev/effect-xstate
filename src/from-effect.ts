import { Cause, Effect, Exit, Fiber, Stream } from "effect";
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

export type FromEffectConfig<A, E, TInput, TEmitted extends EventObject> = {
  readonly effect: (scope: {
    readonly input: TInput;
    readonly emit: (event: TEmitted) => void;
  }) => Effect.Effect<A, E>;
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
export const fromEffect = <
  A,
  E = never,
  TInput = void,
  TEmitted extends EventObject = EventObject,
>(
  config: FromEffectConfig<A, E, TInput, TEmitted>
): ActorLogic<
  EffectActorSnapshot<A, E, TInput>,
  EffectActorEvent<A, E>,
  TInput,
  ActorSystem<ActorSystemInfo>,
  TEmitted
> => {
  const fibers = new WeakMap<object, Fiber.Fiber<unknown, unknown>>();
  return {
    transition: (
      snapshot,
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
        return stopped(settleResult(snapshot.result));
      }
      return snapshot;
    },
    getInitialSnapshot: (_actorScope, input) => active(input),
    start: (snapshot, actorScope) => {
      if (snapshot.status !== "active") {
        return;
      }
      const fiber = Effect.runFork(
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
    },
    getPersistedSnapshot: (snapshot) => snapshot,
  };
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

export type StreamActorSnapshot<A, E, TInput> =
  | {
      readonly status: "active";
      readonly output: undefined;
      readonly error: undefined;
      readonly input: TInput;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "done";
      readonly output: ReadonlyArray<A>;
      readonly error: undefined;
      readonly input: undefined;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "error";
      readonly output: undefined;
      readonly error: Cause.Cause<E>;
      readonly input: undefined;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    }
  | {
      readonly status: "stopped";
      readonly output: undefined;
      readonly error: undefined;
      readonly input: undefined;
      readonly items: ReadonlyArray<A>;
      readonly result: AsyncResult.AsyncResult<A, E>;
    };

export type FromStreamConfig<A, E, TInput, TEmitted extends EventObject> = {
  readonly stream: (scope: {
    readonly input: TInput;
    readonly emit: (event: TEmitted) => void;
  }) => Stream.Stream<A, E>;
};

const streamActive = <A, E, TInput>(
  input: TInput
): StreamActorSnapshot<A, E, TInput> => ({
  status: "active",
  output: undefined,
  error: undefined,
  input,
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
>(
  config: FromStreamConfig<A, E, TInput, TEmitted>
): ActorLogic<
  StreamActorSnapshot<A, E, TInput>,
  StreamActorEvent<A, E>,
  TInput,
  ActorSystem<ActorSystemInfo>,
  TEmitted
> => {
  const fibers = new WeakMap<object, Fiber.Fiber<unknown, unknown>>();
  return {
    transition: (
      snapshot,
      event: StreamInternalEvent<A, E>,
      actorScope
    ) => {
      if (event.type === "stream.next") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        const items = [...snapshot.items, event.value];
        return {
          status: "active",
          output: undefined,
          error: undefined,
          input: snapshot.input,
          items,
          result: AsyncResult.success(event.value, { waiting: true }),
        };
      }
      if (event.type === "stream.done") {
        if (snapshot.status !== "active") {
          return snapshot;
        }
        return {
          status: "done",
          output: snapshot.items,
          error: undefined,
          input: undefined,
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
          input: undefined,
          items: snapshot.items,
          result: AsyncResult.failure(event.cause),
        };
      }
      if (event.type === "xstate.stop") {
        fibers.get(actorScope.self)?.interruptUnsafe();
        fibers.delete(actorScope.self);
        return {
          status: "stopped",
          output: undefined,
          error: undefined,
          input: undefined,
          items: snapshot.items,
          result: settleResult(snapshot.result),
        };
      }
      return snapshot;
    },
    getInitialSnapshot: (_actorScope, input) => streamActive(input),
    start: (snapshot, actorScope) => {
      if (snapshot.status !== "active") {
        return;
      }
      const fiber = Effect.runFork(
        Stream.suspend(() =>
          config.stream({
            input: snapshot.input,
            emit: actorScope.emit,
          })
        )
          .pipe(
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
    },
    getPersistedSnapshot: (snapshot) => snapshot,
  };
};
