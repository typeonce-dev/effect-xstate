# TODO

## Standalone Runtime Error Typing

- Make `runtime.createActor(...)` expose Atom runtime failures in standalone actor snapshot types.
- Stop casting standalone runtime failures into workflow failure types inside `fromEffect` and `fromStream`.
- Add type tests for standalone runtime-backed actors.

## Runtime Availability Semantics

- Add more stress tests for actor stop/disposal while an Atom runtime is still `Initial`.
- Cover Atom runtime failure transitions while actors are waiting for runtime availability.

## Standalone XState Actors

- Expand tests around `runtime.createActor(...)` for streams and machine invokes.
- Cover `runtime.createActor(...)` cleanup when the actor is stopped before an Atom runtime becomes available.

## Atom Registry Coverage

- Expand tests around `fromAtom` with runtime-backed atoms and nested invoked actors.
- Cover registry disposal behavior and multiple actor systems sharing the same atom definitions.

## Shared Error Ergonomics

- Move Cause-backed snapshot helpers out of `from-effect` so `fromEffect`, `fromStream`, and `fromAtom` share one public failure helper surface.
- Add helper tests for extracting and pretty-printing causes from Effect, Stream, and Atom actor snapshots.

## React Runtime Smoke Tests

- Add a minimal React smoke test for `RegistryContext` plus `runtime.actorAtom(...)` running service-backed `fromEffect` and continuous `fromStream` actors.
