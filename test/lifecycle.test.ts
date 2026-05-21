import { Cause, Context, Effect, Layer, Stream } from "effect";
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";
import { createActor, setup, type Actor } from "xstate";
import { describe, expect, it, vi } from "vitest";
import {
  actorAtom,
  actorRefAtom,
  emittedAtom,
  fromAtom,
  fromEffect,
  fromStream,
  persistedAtom,
  runtime as xstateRuntime,
  selectAtom,
} from "../src/main";
import type { ActorAtom } from "../src/main";
import { registerActorSystemRegistry } from "../src/from-atom";
import {
  getActorSystemRuntimeResult,
  registerActorSystemRuntimeContext,
} from "../src/runtime-context";

const waitForStatus = async <S extends { readonly status: string }>(
  actor: { readonly getSnapshot: () => S },
  status: S["status"]
): Promise<S> => {
  await vi.waitFor(() => {
    expect(actor.getSnapshot().status).toBe(status);
  });
  return actor.getSnapshot();
};

describe("runtime lifecycle", () => {
  class PricingService extends Context.Service<
    PricingService,
    { readonly unitPrice: number }
  >()("test/LifecyclePricingService") {}

  it("cleans up a standalone runtime bridge only once", async () => {
    let runtimeFinalizers = 0;
    const runtime = xstateRuntime(
      Atom.runtime(
        Layer.effect(
          PricingService,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                runtimeFinalizers += 1;
              })
            );
            return PricingService.of({ unitPrice: 11 });
          })
        )
      )
    );
    const logic = fromEffect({
      effect: () =>
        Effect.gen(function* () {
          const service = yield* PricingService;
          return service.unitPrice;
        }),
    });
    const actor = runtime.createActor({ logic });

    actor.start();

    await waitForStatus(actor, "done");
    actor.stop();
    actor.stop();
    actor.stop();

    await vi.waitFor(() => {
      expect(runtimeFinalizers).toBe(1);
    });
  });

  it("keeps the newest runtime bridge when an older unregister runs", () => {
    const actor = createActor(fromEffect({ effect: () => Effect.succeed(1) }));
    const first = registerActorSystemRuntimeContext(actor.system, {
      get: () => AsyncResult.success(Context.empty()),
      subscribe: () => () => {},
    });
    const secondContext = Context.make(
      PricingService,
      PricingService.of({ unitPrice: 17 })
    );
    const second = registerActorSystemRuntimeContext(actor.system, {
      get: () => AsyncResult.success(secondContext),
      subscribe: () => () => {},
    });

    first();

    const result = getActorSystemRuntimeResult(actor.system);
    expect(result?._tag).toBe("Success");
    expect(result?._tag === "Success" ? result.value : undefined).toBe(
      secondContext
    );

    second();
    expect(getActorSystemRuntimeResult(actor.system)).toBeUndefined();
  });

  it("keeps the newest Atom registry bridge when an older unregister runs", async () => {
    const atom = Atom.make(1);
    const atomActor = createActor(fromAtom({ atom }));
    const firstRegistry = AtomRegistry.make();
    const secondRegistry = AtomRegistry.make();
    firstRegistry.set(atom, 10);
    secondRegistry.set(atom, 20);
    const first = registerActorSystemRegistry(atomActor.system, firstRegistry);
    const second = registerActorSystemRegistry(atomActor.system, secondRegistry);

    first();
    atomActor.start();

    await vi.waitFor(() => {
      expect(atomActor.getSnapshot().context).toBe(20);
    });

    second();
    atomActor.stop();
  });
});

describe("fromEffect lifecycle", () => {
  class PricingService extends Context.Service<
    PricingService,
    { readonly unitPrice: number }
  >()("test/LifecycleEffectPricingService") {}

  it("removes the pending runtime listener after runtime success", async () => {
    let runtimeResult: AsyncResult.AsyncResult<
      Context.Context<PricingService>,
      never
    > = AsyncResult.initial(true);
    const listeners = new Set<() => void>();
    const started = vi.fn();
    const logic = fromEffect({
      effect: () =>
        Effect.sync(() => {
          started();
          return 1;
        }),
    });
    const actor = createActor(logic);
    const unregister = registerActorSystemRuntimeContext(actor.system, {
      get: () => runtimeResult,
      subscribe: (onChange) => {
        listeners.add(onChange);
        return () => {
          listeners.delete(onChange);
        };
      },
    });

    actor.start();
    expect(listeners.size).toBe(1);

    runtimeResult = AsyncResult.success(
      Context.empty() as Context.Context<PricingService>
    );
    listeners.forEach((listener) => listener());

    await waitForStatus(actor, "done");
    expect(started).toHaveBeenCalledTimes(1);
    expect(listeners.size).toBe(0);

    runtimeResult = AsyncResult.success(
      Context.empty() as Context.Context<PricingService>
    );
    listeners.forEach((listener) => listener());
    await Effect.runPromise(Effect.yieldNow);

    expect(started).toHaveBeenCalledTimes(1);
    expect(listeners.size).toBe(0);
    unregister();
    actor.stop();
  });

  it("removes the pending runtime listener after runtime failure", async () => {
    let runtimeResult: AsyncResult.AsyncResult<
      Context.Context<PricingService>,
      "runtime failed"
    > = AsyncResult.initial(true);
    const listeners = new Set<() => void>();
    const started = vi.fn();
    const logic = fromEffect({
      effect: () =>
        Effect.sync(() => {
          started();
          return 1;
        }),
    });
    const actor = createActor(logic);
    const unregister = registerActorSystemRuntimeContext(actor.system, {
      get: () => runtimeResult,
      subscribe: (onChange) => {
        listeners.add(onChange);
        return () => {
          listeners.delete(onChange);
        };
      },
    });

    actor.subscribe({ error: () => {} });
    actor.start();
    expect(listeners.size).toBe(1);

    runtimeResult = AsyncResult.failure(Cause.fail("runtime failed" as const));
    listeners.forEach((listener) => listener());

    const snapshot = await waitForStatus(actor, "error");
    if (snapshot.status !== "error") {
      throw new Error("Expected error snapshot");
    }
    expect(snapshot.cause).toEqual(Cause.fail("runtime failed"));
    expect(started).not.toHaveBeenCalled();
    expect(listeners.size).toBe(0);

    unregister();
    actor.stop();
  });

  it("handles reentrant runtime readiness during subscription registration", async () => {
    let runtimeResult: AsyncResult.AsyncResult<
      Context.Context<PricingService>,
      never
    > = AsyncResult.initial(true);
    const listeners = new Set<() => void>();
    const started = vi.fn();
    const logic = fromEffect({
      effect: () =>
        Effect.sync(() => {
          started();
          return 1;
        }),
    });
    const actor = createActor(logic);
    const unregister = registerActorSystemRuntimeContext(actor.system, {
      get: () => runtimeResult,
      subscribe: (onChange) => {
        listeners.add(onChange);
        runtimeResult = AsyncResult.success(
          Context.empty() as Context.Context<PricingService>
        );
        onChange();
        return () => {
          listeners.delete(onChange);
        };
      },
    });

    actor.start();

    await waitForStatus(actor, "done");
    expect(started).toHaveBeenCalledTimes(1);
    expect(listeners.size).toBe(0);

    unregister();
    actor.stop();
  });

  it("keeps stopped actors stopped when runtime later fails", async () => {
    let runtimeResult: AsyncResult.AsyncResult<
      Context.Context<PricingService>,
      "runtime failed"
    > = AsyncResult.initial(true);
    const listeners = new Set<() => void>();
    const started = vi.fn();
    const logic = fromEffect({
      effect: () =>
        Effect.sync(() => {
          started();
          return 1;
        }),
    });
    const actor = createActor(logic);
    const unregister = registerActorSystemRuntimeContext(actor.system, {
      get: () => runtimeResult,
      subscribe: (onChange) => {
        listeners.add(onChange);
        return () => {
          listeners.delete(onChange);
        };
      },
    });

    actor.start();
    actor.send({ type: "xstate.stop" });
    expect(actor.getSnapshot().status).toBe("stopped");
    expect(listeners.size).toBe(0);

    runtimeResult = AsyncResult.failure(Cause.fail("runtime failed" as const));
    listeners.forEach((listener) => listener());
    await Effect.runPromise(Effect.yieldNow);

    expect(actor.getSnapshot().status).toBe("stopped");
    expect(started).not.toHaveBeenCalled();
    unregister();
    actor.stop();
  });

  it("keeps done and error snapshots stable when stopped late", async () => {
    const doneActor = createActor(
      fromEffect({
        effect: () => Effect.succeed(1),
      })
    ).start();

    await waitForStatus(doneActor, "done");
    doneActor.stop();
    expect(doneActor.getSnapshot().status).toBe("done");

    const errorActor = createActor(
      fromEffect({
        effect: () => Effect.fail("boom" as const),
      })
    );

    errorActor.subscribe({ error: () => {} });
    errorActor.start();

    await waitForStatus(errorActor, "error");
    errorActor.stop();
    expect(errorActor.getSnapshot().status).toBe("error");

    doneActor.stop();
    errorActor.stop();
  });

  it("does not emit late side-channel events after stop", async () => {
    let emit!: (event: { readonly type: "late.effect" }) => void;
    const emitted = vi.fn();
    const actor = createActor(
      fromEffect({
        effect: (scope: {
          readonly emit: (event: { readonly type: "late.effect" }) => void;
        }) =>
          Effect.sync(() => {
            emit = scope.emit;
          }).pipe(Effect.flatMap(() => Effect.never)),
      })
    );

    actor.on("late.effect", emitted);
    actor.start();
    actor.send({ type: "xstate.stop" });
    emit({ type: "late.effect" });
    await Effect.runPromise(Effect.yieldNow);

    expect(emitted).not.toHaveBeenCalled();
    actor.stop();
  });

  it("interrupts invoked Effect children when the parent leaves the invoking state", async () => {
    let finalized = false;
    const machine = setup({
      types: {
        events: {} as { readonly type: "next" },
      },
      actors: {
        child: fromEffect({
          effect: () =>
            Effect.never.pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finalized = true;
                })
              )
            ),
        }),
      },
    }).createMachine({
      initial: "running",
      states: {
        running: {
          invoke: {
            src: "child",
          },
          on: {
            next: "idle",
          },
        },
        idle: {},
      },
    });
    const actor = createActor(machine).start();

    actor.send({ type: "next" });

    await vi.waitFor(() => {
      expect(finalized).toBe(true);
    });

    actor.stop();
  });

  it("isolates reused Effect logic across multiple actors", async () => {
    let starts = 0;
    let firstRelease!: () => void;
    let secondRelease!: () => void;
    const logic = fromEffect({
      effect: () =>
        Effect.promise(
          () =>
            new Promise<number>((resolve) => {
              starts += 1;
              if (starts === 1) {
                firstRelease = () => resolve(1);
              } else {
                secondRelease = () => resolve(2);
              }
            })
        ),
    });
    const first = createActor(logic);
    const second = createActor(logic);

    first.start();
    second.start();

    first.send({ type: "xstate.stop" });
    secondRelease();
    firstRelease();

    const secondSnapshot = await waitForStatus(second, "done");
    await Effect.runPromise(Effect.yieldNow);

    expect(secondSnapshot.output).toBe(2);
    expect(first.getSnapshot().status).toBe("stopped");
    expect(starts).toBe(2);

    first.stop();
    second.stop();
  });
});

describe("fromStream lifecycle", () => {
  class NumberSource extends Context.Service<
    NumberSource,
    { readonly values: ReadonlyArray<number> }
  >()("test/LifecycleNumberSource") {}

  it("removes the pending runtime listener after stream runtime success", async () => {
    let runtimeResult: AsyncResult.AsyncResult<
      Context.Context<NumberSource>,
      never
    > = AsyncResult.initial(true);
    const listeners = new Set<() => void>();
    const started = vi.fn();
    const logic = fromStream({
      stream: () =>
        Stream.sync(() => {
          started();
          return 1;
        }),
    });
    const actor = createActor(logic);
    const unregister = registerActorSystemRuntimeContext(actor.system, {
      get: () => runtimeResult,
      subscribe: (onChange) => {
        listeners.add(onChange);
        return () => {
          listeners.delete(onChange);
        };
      },
    });

    actor.start();
    expect(listeners.size).toBe(1);

    runtimeResult = AsyncResult.success(
      Context.empty() as Context.Context<NumberSource>
    );
    listeners.forEach((listener) => listener());

    await waitForStatus(actor, "done");
    expect(started).toHaveBeenCalledTimes(1);
    expect(listeners.size).toBe(0);

    runtimeResult = AsyncResult.success(
      Context.empty() as Context.Context<NumberSource>
    );
    listeners.forEach((listener) => listener());
    await Effect.runPromise(Effect.yieldNow);

    expect(started).toHaveBeenCalledTimes(1);
    expect(listeners.size).toBe(0);
    unregister();
    actor.stop();
  });

  it("handles reentrant stream runtime readiness during subscription registration", async () => {
    let runtimeResult: AsyncResult.AsyncResult<
      Context.Context<NumberSource>,
      never
    > = AsyncResult.initial(true);
    const listeners = new Set<() => void>();
    const started = vi.fn();
    const logic = fromStream({
      stream: () =>
        Stream.sync(() => {
          started();
          return 1;
        }),
    });
    const actor = createActor(logic);
    const unregister = registerActorSystemRuntimeContext(actor.system, {
      get: () => runtimeResult,
      subscribe: (onChange) => {
        listeners.add(onChange);
        runtimeResult = AsyncResult.success(
          Context.empty() as Context.Context<NumberSource>
        );
        onChange();
        return () => {
          listeners.delete(onChange);
        };
      },
    });

    actor.start();

    await waitForStatus(actor, "done");
    expect(started).toHaveBeenCalledTimes(1);
    expect(listeners.size).toBe(0);

    unregister();
    actor.stop();
  });

  it("keeps stream done and error snapshots stable when stopped late", async () => {
    const doneActor = createActor(
      fromStream({
        stream: () => Stream.fromIterable([1, 2]),
      })
    ).start();

    await waitForStatus(doneActor, "done");
    doneActor.stop();
    expect(doneActor.getSnapshot().status).toBe("done");

    const errorActor = createActor(
      fromStream({
        stream: () => Stream.fail("boom" as const),
      })
    );

    errorActor.subscribe({ error: () => {} });
    errorActor.start();

    await waitForStatus(errorActor, "error");
    errorActor.stop();
    expect(errorActor.getSnapshot().status).toBe("error");

    doneActor.stop();
    errorActor.stop();
  });

  it("does not emit late side-channel events after stop", async () => {
    let emit!: (event: { readonly type: "late.stream" }) => void;
    const emitted = vi.fn();
    const actor = createActor(
      fromStream({
        stream: (scope: {
          readonly emit: (event: { readonly type: "late.stream" }) => void;
        }) =>
          Stream.unwrap(
            Effect.sync(() => {
              emit = scope.emit;
              return Stream.never;
            })
          ),
      })
    );

    actor.on("late.stream", emitted);
    actor.start();
    actor.send({ type: "xstate.stop" });
    emit({ type: "late.stream" });
    await Effect.runPromise(Effect.yieldNow);

    expect(emitted).not.toHaveBeenCalled();
    actor.stop();
  });

  it("interrupts invoked Stream children when the parent leaves the invoking state", async () => {
    let finalized = false;
    const machine = setup({
      types: {
        events: {} as { readonly type: "next" },
      },
      actors: {
        child: fromStream({
          stream: () =>
            Stream.never.pipe(
              Stream.ensuring(
                Effect.sync(() => {
                  finalized = true;
                })
              )
            ),
        }),
      },
    }).createMachine({
      initial: "running",
      states: {
        running: {
          invoke: {
            src: "child",
          },
          on: {
            next: "idle",
          },
        },
        idle: {},
      },
    });
    const actor = createActor(machine).start();

    actor.send({ type: "next" });

    await vi.waitFor(() => {
      expect(finalized).toBe(true);
    });

    actor.stop();
  });

  it("isolates reused Stream logic across multiple actors", async () => {
    let starts = 0;
    let firstRelease!: () => void;
    let secondRelease!: () => void;
    const logic = fromStream({
      stream: () =>
        Stream.fromEffect(
          Effect.promise(
            () =>
              new Promise<number>((resolve) => {
                starts += 1;
                if (starts === 1) {
                  firstRelease = () => resolve(1);
                } else {
                  secondRelease = () => resolve(2);
                }
              })
          )
        ),
    });
    const first = createActor(logic);
    const second = createActor(logic);

    first.start();
    second.start();

    first.send({ type: "xstate.stop" });
    secondRelease();
    firstRelease();

    const secondSnapshot = await waitForStatus(second, "done");
    await Effect.runPromise(Effect.yieldNow);

    expect(secondSnapshot.items).toEqual([2]);
    expect(first.getSnapshot().status).toBe("stopped");
    expect(starts).toBe(2);

    first.stop();
    second.stop();
  });
});

describe("fromAtom lifecycle", () => {
  it("keeps a stopped snapshot when a subscription failure arrives late", () => {
    const logic = fromAtom({ atom: Atom.make(1) });
    const stopped = {
      status: "stopped" as const,
      output: undefined,
      error: undefined,
      context: 1,
    };
    const next = logic.transition(
      stopped,
      {
        type: "atom.subscription.failed",
        cause: Cause.die(new Error("late subscribe boom")),
      } as unknown as Parameters<typeof logic.transition>[1],
      {
        self: {},
      } as unknown as Parameters<typeof logic.transition>[2]
    );

    expect(next).toBe(stopped);
  });
});

describe("Atom-owned actor lifecycle", () => {
  class LateRuntimeService extends Context.Service<
    LateRuntimeService,
    { readonly value: number }
  >()("test/LifecycleLateRuntimeService") {}

  const counterMachine = setup({
    types: {
      context: {} as { readonly count: number },
      events: {} as { readonly type: "counter.changed"; readonly count: number },
      emitted: {} as {
        readonly type: "counter.changed";
        readonly count: number;
      },
    },
  }).createMachine({
    context: { count: 0 },
  });

  const makeFakeCounterActorAtom = (
    fakeActor: unknown
  ): ActorAtom<typeof counterMachine> =>
    ({
      actor: Atom.make(fakeActor as Actor<typeof counterMachine>),
    }) as unknown as ActorAtom<typeof counterMachine>;

  it("unregisters runtime and registry bridges when actorRefAtom finalizes", async () => {
    const registry = AtomRegistry.make();
    const runtimeAtom = Atom.make<
      AsyncResult.AsyncResult<Context.Context<LateRuntimeService>, never>
    >(AsyncResult.initial(true));
    const started = vi.fn();
    const machine = setup({
      actors: {
        child: fromEffect({
          effect: () =>
            Effect.gen(function* () {
              const service = yield* LateRuntimeService;
              started(service.value);
              return service.value;
            }),
        }),
      },
    }).createMachine({
      invoke: {
        src: "child",
      },
    });
    const actor = actorAtom({ logic: machine, runtime: runtimeAtom });
    const unmount = registry.mount(actor);

    expect(registry.get(actor).status).toBe("active");

    unmount();
    await Effect.runPromise(Effect.yieldNow);
    registry.set(
      runtimeAtom,
      AsyncResult.success(
        Context.make(LateRuntimeService, LateRuntimeService.of({ value: 1 }))
      )
    );
    await Effect.runPromise(Effect.yieldNow);

    expect(started).not.toHaveBeenCalled();
  });

  it("stops an Atom-owned actor once across unmount and registry disposal", async () => {
    const registry = AtomRegistry.make();
    let stops = 0;
    const actor = actorAtom({
      logic: fromEffect({
        effect: () =>
          Effect.never.pipe(
            Effect.ensuring(
              Effect.sync(() => {
                stops += 1;
              })
            )
          ),
      }),
    });
    const unmount = registry.mount(actor);

    registry.get(actor);
    unmount();
    registry.dispose();

    await vi.waitFor(() => {
      expect(stops).toBe(1);
    });
  });

  it("stops a runtime-backed actorAtom once across unmount and registry disposal", async () => {
    class Service extends Context.Service<Service, object>()(
      "test/RuntimeBackedFinalizerService"
    ) {}
    const registry = AtomRegistry.make();
    const runtime = xstateRuntime(Atom.runtime(Layer.succeed(Service, {})));
    let stops = 0;
    const actor = runtime.actorAtom({
      logic: fromEffect({
        effect: () =>
          Effect.gen(function* () {
            yield* Service;
            return yield* Effect.never.pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  stops += 1;
                })
              )
            );
          }),
      }),
    });
    const unmount = registry.mount(actor);

    registry.get(actor);
    unmount();
    registry.dispose();

    await vi.waitFor(() => {
      expect(stops).toBe(1);
    });
  });

  it("provides runtime.actorRefAtom with the same runtime lifecycle bridge", async () => {
    class Service extends Context.Service<
      Service,
      { readonly value: number }
    >()("test/RuntimeActorRefAtomService") {}
    const registry = AtomRegistry.make();
    const runtime = xstateRuntime(
      Atom.runtime(Layer.succeed(Service, Service.of({ value: 42 })))
    );
    const actorRef = runtime.actorRefAtom({
      logic: fromEffect({
        effect: () =>
          Effect.gen(function* () {
            const service = yield* Service;
            return service.value;
          }),
      }),
    });
    const unmount = registry.mount(actorRef);
    const actor = registry.get(actorRef);

    const snapshot = await waitForStatus(actor, "done");
    expect(snapshot.output).toBe(42);

    unmount();
  });

  it("finalizes each runtime-backed invoked child once when the parent stops", async () => {
    class Service extends Context.Service<Service, object>()(
      "test/ManyChildrenRuntimeService"
    ) {}
    const runtime = xstateRuntime(Atom.runtime(Layer.succeed(Service, {})));
    const finalized = new Set<number>();
    const child = (id: number) =>
      fromEffect({
        effect: () =>
          Effect.gen(function* () {
            yield* Service;
            return yield* Effect.never.pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finalized.add(id);
                })
              )
            );
          }),
      });
    const machine = setup({
      actors: {
        one: child(1),
        two: child(2),
        three: child(3),
      },
    }).createMachine({
      invoke: [{ src: "one" }, { src: "two" }, { src: "three" }],
    });
    const actor = runtime.createActor({ logic: machine });

    actor.start();
    actor.stop();

    await vi.waitFor(() => {
      expect(finalized).toEqual(new Set([1, 2, 3]));
    });
  });

  it("unsubscribes selector, emitted, and persisted atoms on finalization", async () => {
    const registry = AtomRegistry.make();
    const selectedUnsubscribe = vi.fn();
    const persistedUnsubscribe = vi.fn();
    const emittedUnsubscribe = vi.fn();
    const fakeActor = {
      getSnapshot: () => ({ context: { count: 0 } }),
      getPersistedSnapshot: () => ({ context: { count: 0 } }),
      subscribe: vi
        .fn()
        .mockReturnValueOnce({ unsubscribe: selectedUnsubscribe })
        .mockReturnValueOnce({ unsubscribe: persistedUnsubscribe }),
      on: vi.fn().mockReturnValue({ unsubscribe: emittedUnsubscribe }),
    };
    const actor = makeFakeCounterActorAtom(fakeActor);
    const selected = selectAtom({
      actor,
      selector: (snapshot: { readonly context: { readonly count: number } }) =>
        snapshot.context.count,
    });
    const emittedEvent = emittedAtom({
      actor,
      type: "counter.changed",
    });
    const persisted = persistedAtom({ actor });
    const unmountSelected = registry.mount(selected);
    const unmountEmitted = registry.mount(emittedEvent);
    const unmountPersisted = registry.mount(persisted);

    registry.get(selected);
    registry.get(emittedEvent);
    registry.get(persisted);

    unmountSelected();
    unmountEmitted();
    unmountPersisted();
    await Effect.runPromise(Effect.yieldNow);

    expect(selectedUnsubscribe).toHaveBeenCalledTimes(1);
    expect(emittedUnsubscribe).toHaveBeenCalledTimes(1);
    expect(persistedUnsubscribe).toHaveBeenCalledTimes(1);
  });

  it("ignores stale actor callbacks after selector, emitted, and persisted finalizers run", async () => {
    const registry = AtomRegistry.make();
    const selectedUnsubscribe = vi.fn();
    const persistedUnsubscribe = vi.fn();
    const emittedUnsubscribe = vi.fn();
    let selectedNext!: (snapshot: { readonly context: { readonly count: number } }) => void;
    let persistedNext!: () => void;
    let emittedNext!: (event: { readonly type: "counter.changed"; readonly count: number }) => void;
    const fakeActor = {
      getSnapshot: () => ({ context: { count: 0 } }),
      getPersistedSnapshot: () => ({ context: { count: 0 } }),
      subscribe: vi
        .fn()
        .mockImplementationOnce((observer) => {
          selectedNext = observer.next;
          return { unsubscribe: selectedUnsubscribe };
        })
        .mockImplementationOnce((observer) => {
          persistedNext = observer.next;
          return { unsubscribe: persistedUnsubscribe };
        }),
      on: vi.fn().mockImplementation((_type, handler) => {
        emittedNext = handler;
        return { unsubscribe: emittedUnsubscribe };
      }),
    };
    const counter = makeFakeCounterActorAtom(fakeActor);
    const selected = selectAtom({
      actor: counter,
      selector: (snapshot) => snapshot.context.count,
    });
    const emittedEvent = emittedAtom({
      actor: counter,
      type: "counter.changed",
    });
    const persisted = persistedAtom({ actor: counter });
    const unmountSelected = registry.mount(selected);
    const unmountEmitted = registry.mount(emittedEvent);
    const unmountPersisted = registry.mount(persisted);

    expect(registry.get(selected)).toBe(0);
    expect(registry.get(persisted)).toMatchObject({ context: { count: 0 } });
    registry.get(emittedEvent);

    unmountSelected();
    unmountEmitted();
    unmountPersisted();
    await Effect.runPromise(Effect.yieldNow);

    expect(() => {
      selectedNext({ context: { count: 1 } });
      emittedNext({ type: "counter.changed", count: 1 });
      persistedNext();
    }).not.toThrow();

    expect(selectedUnsubscribe).toHaveBeenCalledTimes(1);
    expect(emittedUnsubscribe).toHaveBeenCalledTimes(1);
    expect(persistedUnsubscribe).toHaveBeenCalledTimes(1);
  });

  it("reads persisted snapshots for done and error actors", async () => {
    const registry = AtomRegistry.make();
    const doneActor = actorAtom({
      logic: fromEffect({
        effect: () => Effect.succeed(1),
      }),
    });
    const errorActor = actorAtom({
      logic: fromEffect({
        effect: () => Effect.fail("boom" as const),
      }),
    });
    const donePersisted = persistedAtom({ actor: doneActor });
    const errorPersisted = persistedAtom({ actor: errorActor });
    const unmountDone = registry.mount(donePersisted);
    const unmountError = registry.mount(errorPersisted);

    await vi.waitFor(() => {
      expect(registry.get(donePersisted).status).toBe("done");
      expect(registry.get(errorPersisted).status).toBe("error");
    });

    expect(registry.get(donePersisted)).toMatchObject({
      status: "done",
      output: 1,
    });
    expect(registry.get(errorPersisted)).toMatchObject({
      status: "error",
    });

    unmountError();
    unmountDone();
  });
});
