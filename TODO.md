# TODO

## Core

### Standalone Runtime Error Typing

- Make `runtime.createActor(...)` expose Atom runtime failures in standalone actor snapshot types.
- Stop casting standalone runtime failures into workflow failure types inside `fromEffect` and `fromStream`.

## Tests

### Standalone Runtime Actors

- Add type tests for standalone runtime-backed actors.
- Expand tests around `runtime.createActor(...)` for streams and machine invokes.
- Cover `runtime.createActor(...)` cleanup when the actor is stopped before an Atom runtime becomes available.

### Atom Registry

- Expand tests around `fromAtom` with runtime-backed atoms and nested invoked actors.
- Cover registry disposal behavior and multiple actor systems sharing the same atom definitions.

## Docs

- Document standalone runtime error typing once `runtime.createActor(...)` exposes Atom runtime failures in snapshot types.

## React

- Add a minimal React smoke test for `RegistryContext` plus `runtime.actorAtom(...)` running service-backed `fromEffect` and continuous `fromStream` actors.

## Optional

- Add examples for standalone runtime-backed actors after the runtime snapshot error typing is settled.
