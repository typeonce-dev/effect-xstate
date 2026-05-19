# Agent Instructions

This library is an integration of `xstate` actors/machines using the `Atom` module of `effect`. The goal is to allow the use of `xstate` API for state management with `Atom` as a bridge inside components.

This project keeps source references under `.repos/`.

- Check `.repos/effect` when working with Effect APIs, internals, examples, or type definitions.
- Check `.repos/xstate` when working with XState APIs, internals, examples, or type definitions.
- Treat those folders as upstream reference sources. Do not edit them unless the task explicitly asks to update the vendored subtree.

Run `pnpm run typecheck`, `pnpm run test-types` and `pnpm run test` to verify the work before completion.