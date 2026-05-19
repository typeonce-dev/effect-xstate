# TODO

## Runtime Typing

- Connect service requirements from `fromEffect` and `fromStream` to `actorAtom`.
- Ideally, a machine that invokes service-requiring actors should require `runtime(Atom.runtime(layer)).actorAtom(...)` or an equivalent runtime-bearing constructor.
- Plain `actorAtom({ logic })` currently typechecks even when invoked Effect actors need services, so missing runtime support is only caught at runtime.

## Runtime Bridge Lifecycle

- Add explicit cleanup for actor-system runtime bridge entries when the root Atom-owned actor is finalized.
- The current bridge uses a `WeakMap`, so it should not prevent garbage collection, but an explicit unregister would make lifecycle ownership clearer.
- Keep the bridge internal and scoped. Do not use global prototype mutation or patch Effect/XState objects.

## Runtime Availability Semantics

- Revisit what happens when an Atom runtime is still `Initial` while `fromEffect` or `fromStream` starts.
- Current behavior turns unavailable runtime context into an error cause.
- Consider waiting for runtime readiness instead, especially for async layer construction.

## Stream Snapshot Policy

- `fromStream` currently accumulates every emitted item in `snapshot.items`.
- Add policy options for long-running streams:
  - max item buffer
  - latest-only snapshots
  - custom reducer/accumulator
  - emit-only mode with no item accumulation
- This matters for infinite streams and UI subscriptions that should not grow memory forever.

## Standalone XState Actors

- Runtime services only flow automatically through Atom-owned actors.
- Direct `createActor(fromEffect(...))` and `createActor(fromStream(...))` do not have a runtime context unless the effect has no service requirements.
- Consider an explicit runtime option or helper for standalone XState usage.

## Atom Registry Coverage

- Expand tests around `fromAtom` with runtime-backed atoms and nested invoked actors.
- Cover registry disposal behavior and multiple actor systems sharing the same atom definitions.
- Keep explicit registry override behavior well-defined against the active `actorAtom` registry.

## React Runtime Smoke Tests

- Add a small runtime smoke test for the React integration.
- It should verify that a React `RegistryContext` plus `runtime.actorAtom(...)` can run service-backed `fromEffect` and continuous `fromStream` actors.
- Prefer a minimal test that catches provider/runtime wiring regressions without depending on the full example app.
