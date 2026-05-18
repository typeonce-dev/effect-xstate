import { Cause, Effect, Option, Stream } from "effect";
import { Atom } from "effect/unstable/reactivity";
import { describe, expect, it } from "tstyche";
import {
  type EmittedFrom,
  type EventFromLogic,
  type InputFrom,
  setup,
  type SnapshotFrom,
} from "xstate";
import {
  actorAtom,
  emittedAtom,
  type EffectActorEvent,
  type EffectActorSnapshot,
  fromAtom,
  type AtomActorEvent,
  type AtomActorSnapshot,
  fromEffect,
  fromStream,
  type StreamActorEvent,
  type StreamActorSnapshot,
} from "@effect/xstate";

describe("fromEffect", () => {
  it("infers input, output, failure, and emitted event types", () => {
    const logic = fromEffect({
      effect: (scope: {
        readonly input: { readonly quantity: number };
        readonly emit: (event: {
          readonly type: "quote.calculated";
          readonly total: number;
        }) => void;
      }) => {
        expect(scope.input.quantity).type.toBe<number>();
        expect(scope.emit).type.toBeCallableWith({
          type: "quote.calculated",
          total: 12,
        });
        expect(scope.emit).type.not.toBeCallableWith({
          type: "quote.rejected",
        });
        return Effect.succeed(scope.input.quantity * 12);
      },
    });

    expect<InputFrom<typeof logic>>().type.toBe<{
      readonly quantity: number;
    }>();
    expect<SnapshotFrom<typeof logic>>().type.toBe<
      EffectActorSnapshot<number, never, { readonly quantity: number }>
    >();
    expect<EventFromLogic<typeof logic>>().type.toBe<
      EffectActorEvent<number, never>
    >();
    expect<EventFromLogic<typeof logic>>().type.not.toBeAssignableTo<{
      readonly type: "effect.success";
      readonly value: number;
    }>();
    expect<EmittedFrom<typeof logic>>().type.toBe<{
      readonly type: "quote.calculated";
      readonly total: number;
    }>();
  });

  it("preserves typed failures", () => {
    const logic = fromEffect({
      effect: (_scope: { readonly input: string }) =>
        Effect.fail({ _tag: "PricingError" as const }),
    });

    expect<SnapshotFrom<typeof logic>>().type.toBe<
      EffectActorSnapshot<
        never,
        { _tag: "PricingError" },
        string
      >
    >();
    expect<EventFromLogic<typeof logic>>().type.toBe<
      EffectActorEvent<never, { _tag: "PricingError" }>
    >();
  });
});

describe("fromStream", () => {
  it("infers input, stream item, failure, and emitted event types", () => {
    const logic = fromStream({
      stream: (scope: {
        readonly input: { readonly values: ReadonlyArray<number> };
        readonly emit: (event: {
          readonly type: "stream.item";
          readonly value: number;
        }) => void;
      }) => {
        expect(scope.input.values).type.toBe<ReadonlyArray<number>>();
        expect(scope.emit).type.toBeCallableWith({
          type: "stream.item",
          value: 1,
        });
        expect(scope.emit).type.not.toBeCallableWith({
          type: "stream.item",
          value: "1",
        });
        return Stream.fromIterable(scope.input.values);
      },
    });

    expect<InputFrom<typeof logic>>().type.toBe<{
      readonly values: ReadonlyArray<number>;
    }>();
    expect<SnapshotFrom<typeof logic>>().type.toBe<
      StreamActorSnapshot<number, never, {
        readonly values: ReadonlyArray<number>;
      }>
    >();
    expect<EventFromLogic<typeof logic>>().type.toBe<
      StreamActorEvent<number, never>
    >();
    expect<EventFromLogic<typeof logic>>().type.not.toBeAssignableTo<{
      readonly type: "stream.next";
      readonly value: number;
    }>();
    expect<EmittedFrom<typeof logic>>().type.toBe<{
      readonly type: "stream.item";
      readonly value: number;
    }>();
  });

  it("preserves stream failure types", () => {
    const logic = fromStream({
      stream: () => Stream.fail("stream failed" as const),
    });

    expect<SnapshotFrom<typeof logic>>().type.toBe<
      StreamActorSnapshot<never, "stream failed", void>
    >();
    expect<EventFromLogic<typeof logic>>().type.toBe<
      StreamActorEvent<never, "stream failed">
    >();
  });
});

describe("fromAtom", () => {
  it("turns writable Atoms into actor logic with set events", () => {
    const count = Atom.make(0);
    const logic = fromAtom({ atom: count });

    expect<InputFrom<typeof logic>>().type.toBe<void>();
    expect<SnapshotFrom<typeof logic>>().type.toBe<AtomActorSnapshot<number>>();
    expect<EventFromLogic<typeof logic>>().type.toBe<
      AtomActorEvent<number, number>
    >();
    expect<EventFromLogic<typeof logic>>().type.toBeAssignableTo<
      | { readonly type: "atom.refresh" }
      | { readonly type: "atom.set"; readonly value: number }
      | { readonly type: "xstate.stop" }
    >();
    expect<EventFromLogic<typeof logic>>().type.not.toBeAssignableTo<{
      readonly type: "atom.changed";
      readonly value: number;
    }>();
  });

  it("turns read-only Atoms into actor logic without set payloads", () => {
    const greeting = Atom.make(() => "hello");
    const logic = fromAtom({ atom: greeting });

    expect<SnapshotFrom<typeof logic>>().type.toBe<AtomActorSnapshot<string>>();
    expect<EventFromLogic<typeof logic>>().type.toBe<
      AtomActorEvent<string, never>
    >();
  });
});

describe("actorAtom", () => {
  it("preserves machine snapshot and event types", () => {
    const machine = setup({
      types: {
        context: {} as { readonly count: number },
        events: {} as
          | { readonly type: "increment"; readonly by: number }
          | { readonly type: "reset" },
        emitted: {} as { readonly type: "count.changed"; readonly value: number },
      },
    }).createMachine({
      context: { count: 0 },
      on: {
        increment: {
          actions: ({ event }) => {
            expect(event.by).type.toBe<number>();
          },
        },
        reset: {},
      },
    });

    const actor = actorAtom({ logic: machine });
    const changed = emittedAtom({ actor, type: "count.changed" });

    expect(actor).type.toHaveProperty("actor");
    expect(actor).type.toBeAssignableTo<
      Atom.Writable<SnapshotFrom<typeof machine>, EventFromLogic<typeof machine>>
    >();
    expect(changed).type.toBe<
      Atom.Atom<
        Option.Option<{ readonly type: "count.changed"; readonly value: number }>
      >
    >();
  });

  it("requires options when actor logic requires input", () => {
    const machine = setup({
      types: {
        context: {} as { readonly count: number },
        input: {} as { readonly initial: number },
      },
    }).createMachine({
      context: ({ input }) => ({ count: input.initial }),
    });

    expect(actorAtom).type.toBeCallableWith({
      logic: machine,
      options: { input: { initial: 1 } },
    });
    expect(actorAtom).type.not.toBeCallableWith({ logic: machine });
  });
});

describe("exported event and snapshot types", () => {
  it("model failed Effect snapshots with typed causes", () => {
    expect<
      Extract<
        EffectActorSnapshot<number, "boom", void>,
        { readonly status: "error" }
      >["error"]
    >().type.toBe<Cause.Cause<"boom">>();
  });
});
