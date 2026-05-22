# @typeonce/effect-xstate

Integration between `effect` and `xstate`:
- Create `effect` atoms from `xstate` actors
- Create `xstate` actors from `effect` (atoms, effects, streams)

`effect` atom owns the component-facing reactive graph and actor lifecycle, `xstate` owns machines, actors, invocations, emitted events, delays.

> **Note**: I may *or may not* support this long-ish term. 
>
> Effect may [implement State Machines internally soon](https://github.com/Effect-TS/effect-smol/pull/1945). XState is approaching v6, which may bring completely new integration requirements.
>
> If you are interested in this project, you can use it from `@typeonce/effect-xstate`, or consider copying the code for the APIs you need directly in your codebase.

## Installation

This requires `xstate` v5 and `effect` v4 beta.

```sh
pnpm add @typeonce/effect-xstate
```

```sh
npm install @typeonce/effect-xstate
```

```sh
yarn add @typeonce/effect-xstate
```

`effect` and `xstate` are peer dependencies.

```json
"peerDependencies": {
  "effect": "4.0.0-beta.64",
  "xstate": "^5.19.2"
}
```

## Import

```ts
import {
  actorAtom,
  actorRefAtom,
  emittedAtom,
  failureCause,
  failureValue,
  fromAtom,
  fromEffect,
  fromStream,
  isFailureSnapshot,
  persistedAtom,
  prettyCause,
  runtime,
  selectAtom,
} from "@typeonce/effect-xstate";
```

The package is ESM. The public package entrypoint is
`@typeonce/effect-xstate`.

## fromEffect

`fromEffect` converts an Effect workflow into XState actor logic. It is useful
when a machine should invoke Effect business logic while preserving XState's
input, output, cancellation, and failure protocol.

```ts
import { Effect } from "effect";
import { assign, setup } from "xstate";
import { actorAtom, fromEffect } from "@typeonce/effect-xstate";

const checkoutMachine = setup({
  actors: {
    quoteLogic: fromEffect({
      // Run an Effect as an invoked XState actor.
      effect: ({ input }: { readonly input: { readonly total: number } }) =>
        Effect.succeed({ tax: input.total * 0.22 }),
    }),
  },
  types: {
    context: {} as { readonly total: number; readonly tax: number },
  },
}).createMachine({
  context: { total: 100, tax: 0 },
  invoke: {
    src: "quoteLogic",
    input: ({ context }) => ({ total: context.total }),
    onDone: {
      actions: assign({
        tax: ({ event }) => event.output.tax,
      }),
    },
  },
});

export const checkoutActor = actorAtom({
  logic: checkoutMachine,
});
```

The Effect receives the XState `input` and can return typed output for `onDone`.
It can also fail with typed Effect errors; error snapshots expose the underlying
`Cause`.

Use the `emit` function when the workflow should publish XState emitted events
without storing them in machine context. Stopping the invoked actor interrupts
the running Effect fiber.

## fromAtom

`fromAtom` converts a readable or writable Effect Atom into XState actor logic.
It is useful when an invoked actor should observe state that already lives in the
Atom graph instead of duplicating it in machine context.

```ts
import { Atom } from "effect/unstable/reactivity";
import { assign, setup } from "xstate";
import { actorAtom, fromAtom } from "@typeonce/effect-xstate";

const quantity = Atom.make(1);

const checkoutMachine = setup({
  actors: {
    quantityActor: fromAtom({
      // Invoke Atom state from inside a machine.
      atom: quantity,
    }),
  },
  types: {
    context: {} as { readonly quantity: number },
  },
}).createMachine({
  context: { quantity: 0 },
  invoke: {
    src: "quantityActor",
    onSnapshot: {
      actions: assign({
        quantity: ({ event }) => event.snapshot.context,
      }),
    },
  },
});

export const checkoutActor = actorAtom({
  logic: checkoutMachine,
});
```

Readable atoms produce snapshots with the current value in `context`. Writable
atoms also accept `atom.set` events, and all atoms accept `atom.refresh`.

When `fromAtom` is invoked under an actor created by `actorAtom` or
`actorRefAtom`, it automatically uses the active `AtomRegistry`. For standalone
actors, pass `registry` when you need to share a specific Atom registry.

## fromStream

`fromStream` converts an Effect Stream into XState actor logic. It is useful
when a machine should invoke a long-running or multi-value Effect workflow and
keep each emitted value visible in the actor snapshot.

```ts
import { Stream } from "effect";
import { setup } from "xstate";
import { actorAtom, fromStream } from "@typeonce/effect-xstate";

const checkoutMachine = setup({
  actors: {
    pricesLogic: fromStream({
      // Turn a Stream into an invoked XState actor.
      stream: ({
        input,
      }: {
        readonly input: { readonly prices: ReadonlyArray<number> };
      }) => Stream.fromIterable(input.prices),
      accumulation: {
        mode: "reduce",
        seed: 0,
        reducer: (total, price) => total + price,
      },
    }),
  },
}).createMachine({
  context: { prices: [10, 20, 30] },
  invoke: {
    src: "pricesLogic",
    input: ({ context }) => ({ prices: context.prices }),
  },
});

export const checkoutActor = actorAtom({
  logic: checkoutMachine,
});
```

Stream snapshots track `status`, `latest`, `count`, `items`, `value`, and
`result`. The default accumulation mode is `collect`, which keeps emitted items.

Use `latest` to retain only the last item, `none` to avoid retaining emitted
items, or `reduce` to maintain a custom accumulator. Stopping the invoked actor
interrupts the running Stream fiber.

## actorAtom and actorRefAtom

`actorAtom` wraps XState actor logic as a writable Effect Atom whose value is
the current snapshot. `actorRefAtom` exposes the live XState `Actor` when another
atom, integration, or inspection tool needs direct actor access.

```ts
import { AtomRegistry } from "effect/unstable/reactivity";
import { assign, setup } from "xstate";
import { actorAtom, actorRefAtom } from "@typeonce/effect-xstate";

const counterMachine = setup({
  types: {
    context: {} as { readonly count: number },
    events: {} as { readonly type: "counter.increment" },
  },
}).createMachine({
  context: { count: 0 },
  on: {
    "counter.increment": {
      actions: assign({
        count: ({ context }) => context.count + 1,
      }),
    },
  },
});

export const counterActor = actorAtom({
  // Expose the machine snapshot as a writable Atom.
  logic: counterMachine,
});

export const counterRef = actorRefAtom({
  // Expose the live XState actor as an Atom.
  logic: counterMachine,
});

const registry = AtomRegistry.make();

registry.get(counterActor);
registry.set(counterActor, { type: "counter.increment" });
registry.get(counterRef).send({ type: "counter.increment" });
```

`actorAtom` starts lazily when read by an `AtomRegistry`, sends events when the
atom is written to, and stops the actor when the registry finalizes it.

Use `actorRefAtom` sparingly for lower-level integrations that need actor
methods such as `send`, `subscribe`, or `getPersistedSnapshot`. For component
state, prefer `actorAtom` and derived atoms.

## emittedAtom

`emittedAtom` exposes XState emitted events as an Effect Atom. It is useful for
side channels such as telemetry, notifications, or domain events that should not
be stored as durable machine context.

```ts
import { Option } from "effect";
import { emittedAtom } from "@typeonce/effect-xstate";

export const completedAtom = emittedAtom({
  // Subscribe to emitted events from an actorAtom.
  actor: checkoutActor,
  type: "checkout.completed",
});

const latestCompleted = Option.match(registry.get(completedAtom), {
  onNone: () => undefined,
  onSome: (event) => event,
});
```

The returned atom starts as `Option.none()` and becomes `Option.some(event)`
after a matching emitted event is observed.

Pass a concrete emitted event type to select one channel, or pass `"*"` to
observe all emitted events supported by the actor logic.

## persistedAtom

`persistedAtom` exposes `actor.getPersistedSnapshot()` as an Effect Atom. It is
useful when persistence or hydration code should react to XState's persisted
snapshot shape from inside the Atom graph.

```ts
import { persistedAtom } from "@typeonce/effect-xstate";

export const persistedCheckoutAtom = persistedAtom({
  // Project XState's persisted snapshot into Atom.
  actor: checkoutActor,
});

const persisted = registry.get(persistedCheckoutAtom);
```

This helper delegates to XState's persisted snapshot API directly. It does not
add encoding, decoding, storage, or restoration behavior.

The atom updates on actor snapshot changes, completion, and error. Persist the
returned value using the storage layer that fits your application.

## selectAtom

`selectAtom` creates an Effect Atom projection over an `actorAtom` snapshot. It
is useful when UI or domain atoms only need a stable derived value instead of the
whole XState snapshot.

```ts
import { selectAtom } from "@typeonce/effect-xstate";

export const canSubmitAtom = selectAtom({
  // Derive a focused Atom from the actor snapshot.
  actor: checkoutActor,
  selector: (snapshot) =>
    snapshot.matches("editing") && snapshot.context.items.length > 0,
});
```

Selectors run against the live actor snapshot and update the derived atom only
when the selected value changes. By default, changes are compared with
`Object.is`.

Pass `equal` for custom equality when the selected value is an object, array, or
other structure that should avoid unnecessary updates.

## runtime

`runtime` wraps `Atom.runtime(layer)` with XState helpers. It is useful when
invoked `fromEffect` or `fromStream` actors need services from an Effect layer
without mutating global Effect or XState objects.

```ts
import { Context, Effect, Layer } from "effect";
import { Atom } from "effect/unstable/reactivity";
import { assign, setup } from "xstate";
import { fromEffect, runtime as xstateRuntime } from "@typeonce/effect-xstate";

class PricingService extends Context.Service<
  PricingService,
  {
    readonly quote: (quantity: number) => Effect.Effect<number>;
  }
>()("app/PricingService") {}

const PricingLive = Layer.succeed(
  PricingService,
  PricingService.of({
    quote: (quantity) => Effect.succeed(quantity * 12),
  })
);

const appRuntime = xstateRuntime(Atom.runtime(PricingLive));

const checkoutMachine = setup({
  actors: {
    quoteLogic: fromEffect({
      // Use a service provided by Atom.runtime(PricingLive).
      effect: ({ input }: { readonly input: { readonly quantity: number } }) =>
        Effect.gen(function* () {
          const pricing = yield* PricingService;
          return yield* pricing.quote(input.quantity);
        }),
    }),
  },
  types: {
    context: {} as { readonly quantity: number; readonly total: number },
  },
}).createMachine({
  context: { quantity: 2, total: 0 },
  invoke: {
    src: "quoteLogic",
    input: ({ context }) => ({ quantity: context.quantity }),
    onDone: {
      actions: assign({
        total: ({ event }) => event.output,
      }),
    },
  },
});

export const appActor = appRuntime.actorAtom({
  // Create an actorAtom backed by the Atom runtime.
  logic: checkoutMachine,
});

const standaloneActor = appRuntime.createActor({
  // Create a standalone XState actor with the same runtime.
  logic: checkoutMachine,
});
```

Use `appRuntime.actorAtom` or `appRuntime.actorRefAtom` when the actor should be
owned by an Atom registry and use the runtime services. Use
`appRuntime.createActor` for standalone XState actors backed by the same runtime.

Runtime-backed `fromEffect` and `fromStream` invocations wait for the Atom
runtime and include runtime failures in error snapshots. The actor-system bridge
is scoped to the actor and cleaned up when the actor stops.

## Other helpers

The remaining exported functions help inspect Cause-backed error snapshots from
`fromEffect`, `fromStream`, and `fromAtom`. They are useful when an invoked actor
fails and you need the original Effect `Cause` for logging, diagnostics, or
typed error handling.

```ts
import { Effect } from "effect";
import { createActor, setup, waitFor } from "xstate";
import {
  failureCause,
  failureValue,
  fromEffect,
  isFailureSnapshot,
  prettyCause,
} from "@typeonce/effect-xstate";

type QuoteError = {
  readonly _tag: "QuoteError";
  readonly message: string;
};

const checkoutMachine = setup({
  actors: {
    quoteLogic: fromEffect({
      // Fail with a typed Effect error stored in the actor snapshot Cause.
      effect: () =>
        Effect.fail({
          _tag: "QuoteError",
          message: "Quantity is no longer available",
        } as const),
    }),
  },
}).createMachine({
  invoke: {
    id: "quote",
    src: "quoteLogic",
  },
});

const actor = createActor(checkoutMachine).start();
const quoteActor = actor.getSnapshot().children.quote;
const snapshot = await waitFor(quoteActor, (snapshot) => {
  // Wait until the child actor reaches a Cause-backed error snapshot.
  return isFailureSnapshot<QuoteError>(snapshot);
});

if (isFailureSnapshot(snapshot)) {
  // Return the Effect Cause stored on the error snapshot.
  const cause = failureCause(snapshot);

  // Read the original Effect Cause for diagnostics or structured handling.
  console.error(cause);

  // Extract the first typed failure value from the Cause, when present.
  const error = failureValue(snapshot);

  // Format the Cause into a human-readable message for logs.
  console.error(prettyCause(cause));

  if (error?._tag === "QuoteError") {
    console.error(error.message);
  }
}
```

`isFailureSnapshot` only checks for `status: "error"`, so use it on snapshots
from actors that expose Effect causes. `failureCause` returns `snapshot.cause`
when available and falls back to `snapshot.error`, while `failureValue` is most
useful for typed `Effect.fail(...)` values.

`prettyCause` delegates to Effect's Cause formatter. Keep the raw Cause when you
need structured data, and format it only at logging or display boundaries.

The React Vite example lives in `examples/react-vite` and demonstrates these
APIs with `@effect/atom-react`.
