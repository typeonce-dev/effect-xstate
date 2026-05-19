import { Cause, Effect, Option, Stream } from "effect";
import { Atom, AtomRegistry } from "effect/unstable/reactivity";
import { createActor, assign, emit, sendTo, setup } from "xstate";
import { describe, expect, it, vi } from "vitest";
import {
  actorAtom,
  emittedAtom,
  fromAtom,
  fromEffect,
  fromStream,
  persistedAtom,
  selectAtom,
} from "../src/main";

const waitForStatus = async <S extends { readonly status: string }>(
  actor: { readonly getSnapshot: () => S },
  status: S["status"]
): Promise<S> => {
  await vi.waitFor(() => {
    expect(actor.getSnapshot().status).toBe(status);
  });
  return actor.getSnapshot();
};

describe("fromEffect", () => {
  it("runs an Effect actor with input, output, and emitted events", async () => {
    const logic = fromEffect({
      effect: (scope: {
        readonly input: { readonly quantity: number };
        readonly emit: (event: {
          readonly type: "quote.calculated";
          readonly total: number;
        }) => void;
      }) =>
        Effect.sync(() => {
          const total = scope.input.quantity * 12;
          scope.emit({ type: "quote.calculated", total });
          return total;
        }),
    });
    const actor = createActor(logic, { input: { quantity: 3 } });
    const emitted = vi.fn();

    actor.on("quote.calculated", emitted);
    actor.start();

    const snapshot = await waitForStatus(actor, "done");
    expect(snapshot.output).toBe(36);
    expect(snapshot.error).toBeUndefined();
    expect(emitted).toHaveBeenCalledWith({
      type: "quote.calculated",
      total: 36,
    });
    expect(actor.getPersistedSnapshot()).toEqual(snapshot);

    actor.stop();
  });

  it("turns Effect failures into error snapshots", async () => {
    const logic = fromEffect({
      effect: () => Effect.fail("pricing unavailable"),
    });
    const actor = createActor(logic);

    actor.subscribe({ error: () => {} });
    actor.start();

    const snapshot = await waitForStatus(actor, "error");
    expect(snapshot.output).toBeUndefined();
    expect(snapshot.error).toEqual(Cause.fail("pricing unavailable"));

    actor.stop();
  });

  it("captures synchronous defects thrown while constructing an Effect", async () => {
    const defect = new Error("boom");
    const logic = fromEffect({
      effect: () => {
        throw defect;
      },
    });
    const actor = createActor(logic);

    actor.subscribe({ error: () => {} });
    actor.start();

    const snapshot = await waitForStatus(actor, "error");
    if (snapshot.error === undefined) {
      throw new Error("Expected Effect defect cause");
    }
    expect(Cause.hasDies(snapshot.error)).toBe(true);
  });

  it("keeps a stopped actor stopped when interruption reports later", async () => {
    let interrupted = false;
    const logic = fromEffect({
      effect: () =>
        Effect.never.pipe(
          Effect.ensuring(
            Effect.sync(() => {
              interrupted = true;
            })
          )
        ),
    });
    const actor = createActor(logic);

    actor.start();
    actor.send({ type: "xstate.stop" });

    await vi.waitFor(() => {
      expect(interrupted).toBe(true);
    });
    expect(actor.getSnapshot().status).toBe("stopped");
    expect(actor.getSnapshot().result.waiting).toBe(false);

    await Effect.runPromise(Effect.yieldNow);
    expect(actor.getSnapshot().status).toBe("stopped");
  });

  it("ignores a late Effect success after an explicit stop", async () => {
    let resolve!: (value: number) => void;
    const value = new Promise<number>((resume) => {
      resolve = resume;
    });
    const logic = fromEffect({
      effect: () => Effect.promise(() => value),
    });
    const actor = createActor(logic);

    actor.start();
    actor.send({ type: "xstate.stop" });
    expect(actor.getSnapshot().status).toBe("stopped");

    resolve(123);
    await Effect.runPromise(Effect.yieldNow);

    expect(actor.getSnapshot()).toMatchObject({
      status: "stopped",
      output: undefined,
      error: undefined,
    });
  });

  it("interrupts the running fiber when XState stops the actor", async () => {
    let interrupted = false;
    const logic = fromEffect({
      effect: () =>
        Effect.never.pipe(
          Effect.ensuring(
            Effect.sync(() => {
              interrupted = true;
            })
          )
        ),
    });
    const actor = createActor(logic);

    actor.start();
    actor.stop();

    await vi.waitFor(() => {
      expect(interrupted).toBe(true);
    });
  });
});

describe("fromStream", () => {
  it("collects stream items and emits side-channel events", async () => {
    const logic = fromStream({
      stream: (scope: {
        readonly input: { readonly values: ReadonlyArray<number> };
        readonly emit: (event: {
          readonly type: "stream.item";
          readonly value: number;
        }) => void;
      }) =>
        Stream.fromIterable(scope.input.values).pipe(
          Stream.tap((value) =>
            Effect.sync(() => {
              scope.emit({ type: "stream.item", value });
            })
          )
        ),
    });
    const actor = createActor(logic, { input: { values: [1, 2, 3] } });
    const emitted = vi.fn();

    actor.on("stream.item", emitted);
    actor.start();

    const snapshot = await waitForStatus(actor, "done");
    expect(snapshot.items).toEqual([1, 2, 3]);
    expect(snapshot.output).toEqual([1, 2, 3]);
    expect(snapshot.result.waiting).toBe(false);
    expect(emitted).toHaveBeenCalledTimes(3);
    expect(emitted).toHaveBeenLastCalledWith({ type: "stream.item", value: 3 });

    actor.stop();
  });

  it("captures synchronous defects thrown while constructing a Stream", async () => {
    const logic = fromStream({
      stream: () => {
        throw new Error("stream boom");
      },
    });
    const actor = createActor(logic);

    actor.subscribe({ error: () => {} });
    actor.start();

    const snapshot = await waitForStatus(actor, "error");
    if (snapshot.error === undefined) {
      throw new Error("Expected Stream defect cause");
    }
    expect(Cause.hasDies(snapshot.error)).toBe(true);
  });

  it("preserves collected items when a stream fails", async () => {
    const logic = fromStream({
      stream: () =>
        Stream.fromIterable([1, 2]).pipe(Stream.concat(Stream.fail("boom"))),
    });
    const actor = createActor(logic);

    actor.subscribe({ error: () => {} });
    actor.start();

    const snapshot = await waitForStatus(actor, "error");
    expect(snapshot.items).toEqual([1, 2]);
    expect(snapshot.output).toBeUndefined();
    expect(snapshot.error).toEqual(Cause.fail("boom"));

    actor.stop();
  });

  it("ignores pending stream values after stop", async () => {
    let resolveNext!: (value: number) => void;
    const next = new Promise<number>((resume) => {
      resolveNext = resume;
    });
    const logic = fromStream({
      stream: () =>
        Stream.fromIterable([1]).pipe(
          Stream.concat(Stream.fromEffect(Effect.promise(() => next)))
        ),
    });
    const actor = createActor(logic);

    actor.start();
    await vi.waitFor(() => {
      expect(actor.getSnapshot().items).toEqual([1]);
    });

    actor.send({ type: "xstate.stop" });
    resolveNext(2);
    await Effect.runPromise(Effect.yieldNow);

    expect(actor.getSnapshot()).toMatchObject({
      status: "stopped",
      items: [1],
      output: undefined,
      error: undefined,
    });
    expect(actor.getSnapshot().result.waiting).toBe(false);
  });
});

describe("fromAtom", () => {
  it("uses the active actorAtom registry without explicitly passing it", async () => {
    const registry = AtomRegistry.make();
    const count = Atom.make(0);
    const countActor = actorAtom({ logic: fromAtom({ atom: count }) });
    const unmount = registry.mount(countActor);

    expect(registry.get(countActor)).toMatchObject({
      status: "active",
      context: 0,
    });

    registry.set(count, 2);
    await vi.waitFor(() => {
      expect(registry.get(countActor).context).toBe(2);
    });

    registry.set(countActor, { type: "atom.set", value: 5 });

    expect(registry.get(count)).toBe(5);
    expect(registry.get(countActor).context).toBe(5);

    unmount();
  });

  it("keeps automatic registries isolated across actorAtom mounts", async () => {
    const leftRegistry = AtomRegistry.make();
    const rightRegistry = AtomRegistry.make();
    const count = Atom.make(0);
    const countActor = actorAtom({ logic: fromAtom({ atom: count }) });
    const unmountLeftCount = leftRegistry.mount(count);
    const unmountRightCount = rightRegistry.mount(count);
    const unmountLeftActor = leftRegistry.mount(countActor);
    const unmountRightActor = rightRegistry.mount(countActor);

    expect(leftRegistry.get(countActor).context).toBe(0);
    expect(rightRegistry.get(countActor).context).toBe(0);

    leftRegistry.set(count, 2);
    await vi.waitFor(() => {
      expect(leftRegistry.get(countActor).context).toBe(2);
    });
    expect(rightRegistry.get(countActor).context).toBe(0);

    rightRegistry.set(count, 10);
    await vi.waitFor(() => {
      expect(rightRegistry.get(countActor).context).toBe(10);
    });
    expect(leftRegistry.get(countActor).context).toBe(2);

    leftRegistry.set(countActor, { type: "atom.set", value: 3 });

    expect(leftRegistry.get(count)).toBe(3);
    expect(rightRegistry.get(count)).toBe(10);
    expect(leftRegistry.get(countActor).context).toBe(3);
    expect(rightRegistry.get(countActor).context).toBe(10);

    unmountRightActor();
    unmountLeftActor();
    unmountRightCount();
    unmountLeftCount();
  });

  it("uses the actorAtom registry for fromAtom actors invoked by a machine", async () => {
    const registry = AtomRegistry.make();
    const count = Atom.make(0);
    const machine = setup({
      types: {
        context: {} as { readonly observed: number },
        events: {} as { readonly type: "count.set"; readonly value: number },
      },
      actors: {
        count: fromAtom({ atom: count }),
      },
    }).createMachine({
      context: { observed: -1 },
      invoke: {
        id: "count",
        src: "count",
        onSnapshot: {
          actions: assign({
            observed: ({ event }) => event.snapshot.context,
          }),
        },
      },
      on: {
        "count.set": {
          actions: sendTo("count", ({ event }) => ({
            type: "atom.set",
            value: event.value,
          })),
        },
      },
    });
    const machineAtom = actorAtom({ logic: machine });
    const unmount = registry.mount(machineAtom);

    await vi.waitFor(() => {
      expect(registry.get(machineAtom).context.observed).toBe(0);
    });

    registry.set(count, 3);
    await vi.waitFor(() => {
      expect(registry.get(machineAtom).context.observed).toBe(3);
    });

    registry.set(machineAtom, { type: "count.set", value: 7 });

    expect(registry.get(count)).toBe(7);
    await vi.waitFor(() => {
      expect(registry.get(machineAtom).context.observed).toBe(7);
    });

    unmount();
  });

  it("keeps an explicit fromAtom registry as an override", async () => {
    const actorRegistry = AtomRegistry.make();
    const explicitRegistry = AtomRegistry.make();
    const count = Atom.make(0);
    explicitRegistry.set(count, 10);
    const unmountActorCount = actorRegistry.mount(count);
    actorRegistry.set(count, 1);
    const countActor = actorAtom({
      logic: fromAtom({ atom: count, registry: explicitRegistry }),
    });
    const unmount = actorRegistry.mount(countActor);

    expect(actorRegistry.get(countActor).context).toBe(10);

    actorRegistry.set(count, 20);
    await Effect.runPromise(Effect.yieldNow);
    expect(actorRegistry.get(countActor).context).toBe(10);

    explicitRegistry.set(count, 11);
    await vi.waitFor(() => {
      expect(actorRegistry.get(countActor).context).toBe(11);
    });

    actorRegistry.set(countActor, { type: "atom.set", value: 12 });

    expect(explicitRegistry.get(count)).toBe(12);
    expect(actorRegistry.get(count)).toBe(20);

    unmount();
    unmountActorCount();
  });

  it("uses a stable private registry when created outside actorAtom", () => {
    let runs = 0;
    const value = Atom.make(() => {
      runs += 1;
      return runs;
    });
    const actor = createActor(fromAtom({ atom: value })).start();

    expect(actor.getSnapshot().context).toBe(1);

    actor.send({ type: "atom.refresh" });

    expect(actor.getSnapshot().context).toBe(2);

    actor.stop();
  });

  it("mirrors writable Atom changes in both directions", async () => {
    const registry = AtomRegistry.make();
    const count = Atom.make(0);
    const actor = createActor(fromAtom({ atom: count, registry }));

    actor.start();

    expect(actor.getSnapshot()).toMatchObject({
      status: "active",
      context: 0,
    });

    registry.set(count, 2);
    await vi.waitFor(() => {
      expect(actor.getSnapshot().context).toBe(2);
    });

    actor.send({ type: "atom.set", value: 5 });
    expect(registry.get(count)).toBe(5);
    expect(actor.getSnapshot().context).toBe(5);

    actor.stop();
  });

  it("re-reads the Atom value when the actor starts", () => {
    const registry = AtomRegistry.make();
    const count = Atom.make(0);
    const actor = createActor(fromAtom({ atom: count, registry }));

    registry.set(count, 5);
    actor.start();

    expect(actor.getSnapshot()).toMatchObject({
      status: "active",
      context: 5,
    });

    actor.stop();
  });

  it("refreshes derived Atoms on demand", () => {
    let runs = 0;
    const registry = AtomRegistry.make();
    const value = Atom.make(() => {
      runs += 1;
      return runs;
    });
    const actor = createActor(fromAtom({ atom: value, registry })).start();

    expect(actor.getSnapshot().context).toBe(1);

    actor.send({ type: "atom.refresh" });

    expect(actor.getSnapshot().context).toBe(2);

    actor.stop();
  });

  it("unsubscribes from Atom updates after stop", async () => {
    const registry = AtomRegistry.make();
    const count = Atom.make(0);
    const actor = createActor(fromAtom({ atom: count, registry })).start();

    actor.send({ type: "xstate.stop" });
    expect(actor.getSnapshot()).toMatchObject({
      status: "stopped",
      context: 0,
    });

    registry.set(count, 10);
    await Effect.runPromise(Effect.yieldNow);

    expect(actor.getSnapshot()).toMatchObject({
      status: "stopped",
      context: 0,
    });
  });

  it("ignores manual Atom events once stopped", () => {
    const registry = AtomRegistry.make();
    const count = Atom.make(0);
    const actor = createActor(fromAtom({ atom: count, registry })).start();

    actor.send({ type: "xstate.stop" });
    actor.send({ type: "atom.set", value: 20 });
    actor.send({ type: "atom.refresh" });

    expect(actor.getSnapshot()).toMatchObject({
      status: "stopped",
      context: 0,
    });
    expect(registry.get(count)).toBe(0);
  });
});

describe("actorAtom", () => {
  const counterMachine = setup({
    types: {
      context: {} as { readonly count: number },
      events: {} as { readonly type: "counter.increment"; readonly by: number },
      emitted: {} as {
        readonly type: "counter.changed";
        readonly count: number;
      },
    },
  }).createMachine({
    context: { count: 0 },
    on: {
      "counter.increment": {
        actions: [
          assign({
            count: ({ context, event }) => context.count + event.by,
          }),
          emit(({ context }) => ({
            type: "counter.changed",
            count: context.count,
          })),
        ],
      },
    },
  });

  it("exposes an XState actor as a writable Atom", () => {
    const registry = AtomRegistry.make();
    const counter = actorAtom({ logic: counterMachine });
    const unmount = registry.mount(counter);

    expect(registry.get(counter).context.count).toBe(0);

    registry.set(counter, { type: "counter.increment", by: 2 });

    expect(registry.get(counter).context.count).toBe(2);

    unmount();
  });

  it("selects snapshots, emitted events, and XState persisted snapshots", () => {
    const registry = AtomRegistry.make();
    const counter = actorAtom({ logic: counterMachine });
    const selected = selectAtom({
      actor: counter,
      selector: (snapshot) => snapshot.context.count,
    });
    const emittedEvent = emittedAtom({
      actor: counter,
      type: "counter.changed",
    });
    const persisted = persistedAtom({ actor: counter });
    const unmountCounter = registry.mount(counter);
    const unmountSelected = registry.mount(selected);
    const unmountEmitted = registry.mount(emittedEvent);
    const unmountPersisted = registry.mount(persisted);

    expect(registry.get(selected)).toBe(0);
    expect(Option.isNone(registry.get(emittedEvent))).toBe(true);

    registry.set(counter, { type: "counter.increment", by: 4 });

    expect(registry.get(selected)).toBe(4);
    expect(registry.get(emittedEvent)).toEqual(
      Option.some({ type: "counter.changed", count: 4 })
    );
    expect(registry.get(persisted)).toMatchObject({
      context: { count: 4 },
    });

    unmountPersisted();
    unmountEmitted();
    unmountSelected();
    unmountCounter();
  });
});
