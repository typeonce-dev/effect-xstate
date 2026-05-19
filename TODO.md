# TODO

## Runtime Availability Semantics

- Add more stress tests for actor stop/disposal while an Atom runtime is still `Initial`.

## Standalone XState Actors

- Expand tests around `runtime.createActor(...)` for streams and machine invokes.

## Atom Registry Coverage

- Expand tests around `fromAtom` with runtime-backed atoms and nested invoked actors.
- Cover registry disposal behavior and multiple actor systems sharing the same atom definitions.
- Keep explicit registry override behavior well-defined against the active `actorAtom` registry.

## React Runtime Smoke Tests

- Add a small runtime smoke test for the React integration.
- It should verify that a React `RegistryContext` plus `runtime.actorAtom(...)` can run service-backed `fromEffect` and continuous `fromStream` actors.
- Prefer a minimal test that catches provider/runtime wiring regressions without depending on the full example app.
