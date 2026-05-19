# @effect/xstate

Experimental bridge between Effect Atom and XState actors.

`actorAtom` makes the Effect Atom registry own actor lifecycle and subscription invalidation, while XState remains the owner of machine snapshots, events, child actors, delayed transitions, and persistence.

```ts
const checkoutActor = actorAtom({
  logic: checkoutMachine,
});

const canSubmitAtom = selectAtom({
  actor: checkoutActor,
  selector: (snapshot) =>
    snapshot.matches("ready") && snapshot.context.items.length > 0,
});
```

Effect can also become XState actor logic:

```ts
const quoteActor = fromEffect({
  effect: (scope: { readonly input: { readonly total: number } }) =>
    Effect.succeed({ total: scope.input.total }),
});
```

When invoked actors need Effect services, wrap an `Atom.runtime` once and create
the actor atom from that runtime:

```ts
import { runtime as xstateRuntime } from "@effect/xstate";

const runtime = xstateRuntime(Atom.runtime(PricingLive));

const checkoutActor = runtime.actorAtom({
  logic: checkoutMachine,
});
```

Service requirements are type-checked at the actor boundary. A machine that
invokes service-backed `fromEffect` or `fromStream` logic must be created from a
compatible `runtime.actorAtom(...)`, or from `runtime.createActor(...)` for
standalone XState usage.

Effect failures are exposed as typed `Cause` values:

```ts
const snapshot = actor.getSnapshot();

if (isFailureSnapshot(snapshot)) {
  console.log(prettyCause(failureCause(snapshot)));
}
```

Streams support explicit accumulation policies for long-running actors:

```ts
const ticker = fromStream({
  stream: () => Stream.fromIterable([1, 2, 3]),
  accumulation: { mode: "collect", maxItems: 100 },
});

const total = fromStream({
  stream: () => Stream.fromIterable([1, 2, 3]),
  accumulation: {
    mode: "reduce",
    seed: 0,
    reducer: (sum, value) => sum + value,
  },
});
```

Atoms can be invoked as actors too:

```ts
const count = Atom.make(0);
const countActor = createActor(fromAtom({ atom: count }));

countActor.start();
countActor.send({ type: "atom.set", value: 1 });
```

## React Vite example

The React showcase lives outside the package source:

```sh
cd packages/actor-atom/examples/react-vite
pnpm install
pnpm dev
```

It is a standalone Vite app that imports the bridge through local source paths, uses `@effect/atom-react` for React hooks, and demonstrates `actorAtom`, `selectAtom`, `emittedAtom`, `persistedAtom`, `fromAtom`, and `fromEffect`.
