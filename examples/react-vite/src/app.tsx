import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Option } from "effect";
import { useRef, type ReactElement } from "react";
import {
  canPayAtom,
  canRequestQuoteAtom,
  checkoutActor,
  checkoutStatusAtom,
  paymentAtom,
  persistedCheckoutAtom,
  quantityAtom,
  quoteAtom,
  tickerAtom,
} from "./domain";

const formatCurrency = (config: { readonly value: number }): string =>
  new Intl.NumberFormat("en-US", {
    currency: "USD",
    style: "currency",
  }).format(config.value);

const RenderCount = (props: { readonly label: string }): ReactElement => {
  const count = useRef(0);
  count.current += 1;
  return (
    <span className="render-count">
      {props.label} rendered {count.current} times
    </span>
  );
};

export const App = (): ReactElement => {
  const quantity = useAtomValue(quantityAtom);
  const setQuantity = useAtomSet(quantityAtom);
  const status = useAtomValue(checkoutStatusAtom);
  const quote = useAtomValue(quoteAtom);
  const canRequestQuote = useAtomValue(canRequestQuoteAtom);
  const canPay = useAtomValue(canPayAtom);
  const payment = useAtomValue(paymentAtom);
  const persisted = useAtomValue(persistedCheckoutAtom);
  const ticker = useAtomValue(tickerAtom);
  const send = useAtomSet(checkoutActor);
  return (
    <main className="shell">
      <section className="hero">
        <p className="eyebrow">Effect Atom + XState</p>
        <h1>Small checkout flow</h1>
        <p>
          Quantity is an Effect Atom. The machine reads it through fromAtom,
          prices it through a custom Atom runtime service, and React reads the
          machine through normal Atom hooks.
        </p>
      </section>
      <section className="grid">
        <div className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">fromAtom</p>
              <h2>Quantity atom</h2>
            </div>
            <RenderCount label="quantity" />
          </div>
          <div className="quantity-row">
            <button
              className="icon-button"
              onClick={() => {
                setQuantity((current) => Math.max(0, current - 1));
              }}
            >
              -
            </button>
            <strong>{quantity}</strong>
            <button
              className="icon-button"
              onClick={() => {
                setQuantity((current) => current + 1);
              }}
            >
              +
            </button>
          </div>
          <p className="muted">
            Changing this atom sends a snapshot into the invoked XState actor
            and resets the quote.
          </p>
        </div>
        <div className="panel primary-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">actorAtom + selectAtom</p>
              <h2>Checkout machine</h2>
            </div>
            <RenderCount label="machine selectors" />
          </div>
          <div className="metric-row">
            <div className="metric">
              <span>State</span>
              <strong>{status}</strong>
            </div>
            <div className="metric">
              <span>Quote</span>
              <strong>
                {quote === null
                  ? "None"
                  : formatCurrency({ value: quote.total })}
              </strong>
            </div>
            <div className="metric">
              <span>Runtime service</span>
              <strong>
                {quote === null
                  ? "Ready"
                  : `${formatCurrency({ value: quote.unitPrice })} / unit`}
              </strong>
            </div>
          </div>
          <div className="action-row">
            <button
              className="primary-button"
              disabled={!canRequestQuote}
              onClick={() => {
                send({ type: "quote.requested" });
              }}
            >
              Request quote
            </button>
            <button
              className="primary-button"
              disabled={!canPay}
              onClick={() => {
                send({ type: "payment.confirmed" });
              }}
            >
              Pay
            </button>
            <button
              className="secondary-button"
              onClick={() => {
                send({ type: "checkout.reset" });
              }}
            >
              Reset
            </button>
          </div>
        </div>
        <div className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">fromStream</p>
              <h2>Scheduled stream</h2>
            </div>
            <RenderCount label="ticker" />
          </div>
          <div className="metric-row compact">
            <div className="metric">
              <span>Latest</span>
              <strong>{ticker.latest}</strong>
            </div>
            <div className="metric">
              <span>Items</span>
              <strong>{ticker.count}</strong>
            </div>
          </div>
          <ul className="activity-list">
            {ticker.recent.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
        <div className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">emittedAtom + persistedAtom</p>
              <h2>Actor side channels</h2>
            </div>
            <RenderCount label="side channels" />
          </div>
          <p className="event-box">
            {Option.isSome(payment)
              ? `Payment submitted for ${formatCurrency({ value: payment.value.total })}`
              : "No payment event yet"}
          </p>
          <pre className="snapshot-box">
            {JSON.stringify(persisted, null, 2)}
          </pre>
        </div>
      </section>
    </main>
  );
};
