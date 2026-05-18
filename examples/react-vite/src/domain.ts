import {
  actorAtom,
  emittedAtom,
  fromAtom,
  fromEffect,
  persistedAtom,
  selectAtom,
} from "@effect/xstate";
import { Effect } from "effect";
import { Atom, AtomRegistry } from "effect/unstable/reactivity";
import { assign, emit, setup } from "xstate";

export type Quote = {
  readonly quantity: number;
  readonly unitPrice: number;
  readonly total: number;
};

export type PaymentSubmittedEvent = {
  readonly type: "payment.submitted";
  readonly total: number;
};

type CheckoutContext = {
  readonly quantity: number;
  readonly quote: Quote | null;
};

type CheckoutEvent =
  | { readonly type: "quote.requested" }
  | { readonly type: "payment.confirmed" }
  | { readonly type: "checkout.reset" };

export const registry = AtomRegistry.make();

export const quantityAtom = Atom.make(1);

export const checkoutMachine = setup({
  types: {
    context: {} as CheckoutContext,
    events: {} as CheckoutEvent,
    emitted: {} as PaymentSubmittedEvent,
  },
  actors: {
    quantity: fromAtom({
      atom: quantityAtom,
      registry,
    }),
    pricing: fromEffect({
      effect: (scope: { readonly input: { readonly quantity: number } }) =>
        Effect.succeed({
          quantity: scope.input.quantity,
          unitPrice: 12,
          total: scope.input.quantity * 12,
        }).pipe(Effect.delay("350 millis")),
    }),
  },
}).createMachine({
  id: "actor-atom-simple-checkout",
  context: {
    quantity: 1,
    quote: null,
  },
  invoke: {
    src: "quantity",
    onSnapshot: {
      target: ".editing",
      actions: assign({
        quantity: ({ event }) => event.snapshot.context,
        quote: null,
      }),
    },
  },
  initial: "editing",
  states: {
    editing: {
      on: {
        "quote.requested": {
          target: "pricing",
          guard: ({ context }) => context.quantity > 0,
        },
      },
    },
    pricing: {
      invoke: {
        src: "pricing",
        input: ({ context }) => ({ quantity: context.quantity }),
        onDone: {
          target: "quoted",
          actions: assign({
            quote: ({ event }) => event.output,
          }),
        },
        onError: {
          target: "editing",
        },
      },
    },
    quoted: {
      on: {
        "quote.requested": {
          target: "pricing",
        },
        "payment.confirmed": {
          target: "paid",
        },
      },
    },
    paid: {
      entry: emit(({ context }) => ({
        type: "payment.submitted",
        total: context.quote?.total ?? 0,
      })),
      on: {
        "checkout.reset": {
          target: "editing",
        },
      },
    },
  },
});

export const checkoutActor = actorAtom({
  logic: checkoutMachine,
});

export const checkoutStatusAtom = selectAtom({
  actor: checkoutActor,
  selector: (snapshot) => String(snapshot.value),
});

export const quoteAtom = selectAtom({
  actor: checkoutActor,
  selector: (snapshot) => snapshot.context.quote,
});

export const canRequestQuoteAtom = selectAtom({
  actor: checkoutActor,
  selector: (snapshot) =>
    (snapshot.matches("editing") || snapshot.matches("quoted")) &&
    snapshot.context.quantity > 0,
});

export const canPayAtom = selectAtom({
  actor: checkoutActor,
  selector: (snapshot) =>
    snapshot.matches("quoted") && snapshot.context.quote !== null,
});

export const paymentAtom = emittedAtom({
  actor: checkoutActor,
  type: "payment.submitted",
});

export const persistedCheckoutAtom = persistedAtom({
  actor: checkoutActor,
});
