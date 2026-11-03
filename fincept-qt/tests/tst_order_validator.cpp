// Unit tests for src/trading/OrderValidator.{h,cpp}
//
// OrderValidator.cpp includes only its own header, which includes only
// trading/TradingTypes.h (Qt Core types + plain aggregate structs, no .cpp of
// its own). That is why this suite costs exactly one extra translation unit and
// links nothing from the broker registry, the HTTP stack or any service. The
// stateful half of the risk gate (settings I/O, account snapshot,
// pre_trade_risk) lives in OrderValidatorRiskGate.cpp and is not linked here;
// check_risk() is pure and is covered below.
//
// The headline case is validate_smart()'s FLATTEN order: position_size == 0 is a
// legitimate target ("close me out"), and the engine derives the quantity from
// the live book — so quantity == 0 alongside it must validate. A rule that
// required a positive quantity here would have rejected every flatten order the
// terminal ever sends.

#include "trading/OrderValidator.h"
#include "trading/TradingTypes.h"

#include <QString>
#include <QStringList>
#include <QTest>

using fincept::trading::BasketOrderRequest;
using fincept::trading::OrderSide;
using fincept::trading::OrderType;
using fincept::trading::OrderValidator;
using fincept::trading::ProductType;
using fincept::trading::SmartOrder;
using fincept::trading::UnifiedOrder;

using Validation = OrderValidator::ValidationResult;

static bool has_error(const Validation& r, const QString& needle) {
    for (const QString& e : r.errors) {
        if (e.contains(needle))
            return true;
    }
    return false;
}

static QString joined(const Validation& r) {
    return r.errors.join(QStringLiteral(" | "));
}

// A minimal order that must validate, so each test can perturb exactly one field.
static UnifiedOrder good_order() {
    UnifiedOrder o;
    o.symbol = QStringLiteral("RELIANCE");
    o.exchange = QStringLiteral("NSE");
    o.side = OrderSide::Buy;
    o.order_type = OrderType::Market;
    o.quantity = 10;
    o.product_type = ProductType::Intraday;
    return o;
}

static SmartOrder good_smart() {
    SmartOrder o;
    o.symbol = QStringLiteral("RELIANCE");
    o.exchange = QStringLiteral("NSE");
    o.action = OrderSide::Buy;
    o.order_type = OrderType::Market;
    o.quantity = 10;
    o.position_size = 100;
    return o;
}

class TstOrderValidator : public QObject {
    Q_OBJECT

  private slots:
    // validate()
    void accepts_a_well_formed_market_order();
    void rejects_missing_or_blank_symbol();
    void rejects_missing_exchange();
    void rejects_unknown_exchange();
    void exchange_whitelist_spot_checks();
    void exchange_match_is_case_insensitive_but_not_whitespace_tolerant();
    void rejects_non_positive_quantity();
    void price_required_only_for_limit_family();
    void trigger_required_only_for_stop_family();
    void stop_loss_limit_requires_both_price_and_trigger();
    void errors_accumulate();

    // validate_smart()
    void smart_flatten_with_zero_quantity_is_valid();
    void smart_allows_zero_quantity_for_any_position_size();
    void smart_rejects_negative_quantity();
    void smart_validates_symbol_and_exchange();
    void smart_price_and_trigger_rules_match_validate();

    // validate_basket()
    void basket_rejects_empty();
    void basket_accepts_all_valid_legs();
    void basket_reports_the_failing_leg_by_index_and_symbol();

    // check_risk() — pre-trade risk gate
    void risk_defaults_pass_a_normal_order();
    void risk_sanity_quantity_ceiling_applies_to_everything();
    void risk_sanity_buying_power_multiple_is_cash_only();
    void risk_configured_limits_skip_reducing_and_paper_orders();
    void risk_blocked_symbols_match_plain_and_exchange_qualified();
    void risk_max_order_quantity_is_inclusive();
    void risk_max_order_value_fails_closed_without_a_price();
    void risk_daily_loss_limit_fails_closed_and_triggers_at_the_limit();
    void derivative_exchange_classification();
};

// ── validate() ───────────────────────────────────────────────────────────────

void TstOrderValidator::accepts_a_well_formed_market_order() {
    const auto r = OrderValidator::validate(good_order());
    QVERIFY2(r.valid, qPrintable(joined(r)));
    QVERIFY(r.errors.isEmpty());
}

void TstOrderValidator::rejects_missing_or_blank_symbol() {
    UnifiedOrder o = good_order();
    o.symbol.clear();
    auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Symbol is required")));

    // Whitespace is trimmed before the emptiness check.
    o.symbol = QStringLiteral("   ");
    r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Symbol is required")));
}

void TstOrderValidator::rejects_missing_exchange() {
    UnifiedOrder o = good_order();
    o.exchange.clear();
    auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Exchange is required")));
    // A blank exchange is "required", not "invalid" — only one of the two fires.
    QVERIFY(!has_error(r, QStringLiteral("Invalid exchange")));

    o.exchange = QStringLiteral("   ");
    r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Exchange is required")));
}

void TstOrderValidator::rejects_unknown_exchange() {
    UnifiedOrder o = good_order();
    o.exchange = QStringLiteral("NOTANEXCHANGE");
    const auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Invalid exchange: NOTANEXCHANGE")));
}

void TstOrderValidator::exchange_whitelist_spot_checks() {
    // India
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("NSE")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("BSE")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("NFO")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("BFO")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("MCX")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("CDS")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("NSE_INDEX")));
    // US / global
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("NYSE")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("NASDAQ")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("LSE")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("XETRA")));
    // crypto / forex
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("CRYPTO")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("FOREX")));

    // Not on the list.
    QVERIFY(!OrderValidator::is_valid_exchange(QString()));
    QVERIFY(!OrderValidator::is_valid_exchange(QStringLiteral("BINANCE")));
    QVERIFY(!OrderValidator::is_valid_exchange(QStringLiteral("NSEINDEX")));
    QVERIFY(!OrderValidator::is_valid_exchange(QStringLiteral("N")));
}

void TstOrderValidator::exchange_match_is_case_insensitive_but_not_whitespace_tolerant() {
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("nse")));
    QVERIFY(OrderValidator::is_valid_exchange(QStringLiteral("Nasdaq")));

    // Documented quirk, pinned so a change to it is deliberate: validate() trims
    // only for the emptiness check and then hands the RAW string to the
    // whitelist, so a padded exchange is rejected as invalid rather than
    // normalised. Callers must trim upstream.
    QVERIFY(!OrderValidator::is_valid_exchange(QStringLiteral(" NSE ")));

    UnifiedOrder o = good_order();
    o.exchange = QStringLiteral(" NSE ");
    const auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Invalid exchange")));
}

void TstOrderValidator::rejects_non_positive_quantity() {
    UnifiedOrder o = good_order();
    o.quantity = 0;
    auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Quantity must be a positive number")));

    o.quantity = -5;
    r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Quantity must be a positive number")));

    // Fractional quantities are permitted (crypto / fractional shares).
    o.quantity = 0.5;
    r = OrderValidator::validate(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));
}

void TstOrderValidator::price_required_only_for_limit_family() {
    UnifiedOrder o = good_order();

    o.order_type = OrderType::Limit;
    o.price = 0;
    auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Price is required")));

    o.price = 2500.75;
    r = OrderValidator::validate(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));

    // Market orders never need a price.
    o.order_type = OrderType::Market;
    o.price = 0;
    r = OrderValidator::validate(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));

    // A negative price is as bad as a missing one.
    o.order_type = OrderType::Limit;
    o.price = -1;
    r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Price is required")));
}

void TstOrderValidator::trigger_required_only_for_stop_family() {
    UnifiedOrder o = good_order();

    o.order_type = OrderType::StopLoss;
    o.stop_price = 0;
    auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Trigger price is required")));

    o.stop_price = 2400;
    r = OrderValidator::validate(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));

    // A plain Market order with no trigger is fine.
    o.order_type = OrderType::Market;
    o.stop_price = 0;
    r = OrderValidator::validate(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));

    // ...and so is a Limit order (trigger belongs to the stop family only).
    o.order_type = OrderType::Limit;
    o.price = 2500;
    r = OrderValidator::validate(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));
}

void TstOrderValidator::stop_loss_limit_requires_both_price_and_trigger() {
    UnifiedOrder o = good_order();
    o.order_type = OrderType::StopLossLimit;
    o.price = 0;
    o.stop_price = 0;

    auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QCOMPARE(r.errors.size(), qsizetype(2));
    QVERIFY(has_error(r, QStringLiteral("Price is required")));
    QVERIFY(has_error(r, QStringLiteral("Trigger price is required")));

    o.price = 2500;
    r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QCOMPARE(r.errors.size(), qsizetype(1));
    QVERIFY(has_error(r, QStringLiteral("Trigger price is required")));

    o.stop_price = 2490;
    r = OrderValidator::validate(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));
}

void TstOrderValidator::errors_accumulate() {
    UnifiedOrder o;
    o.symbol.clear();
    o.exchange = QStringLiteral("NOPE");
    o.order_type = OrderType::Limit;
    o.quantity = 0;
    o.price = 0;

    const auto r = OrderValidator::validate(o);
    QVERIFY(!r.valid);
    QCOMPARE(r.errors.size(), qsizetype(4));
    QVERIFY(has_error(r, QStringLiteral("Symbol is required")));
    QVERIFY(has_error(r, QStringLiteral("Invalid exchange: NOPE")));
    QVERIFY(has_error(r, QStringLiteral("Quantity must be a positive number")));
    QVERIFY(has_error(r, QStringLiteral("Price is required")));
}

// ── validate_smart() ─────────────────────────────────────────────────────────

void TstOrderValidator::smart_flatten_with_zero_quantity_is_valid() {
    // THE case this suite exists for. "Flatten": target net position 0, quantity
    // left at 0 because the engine reads the live book and derives it. Requiring
    // a positive quantity here rejects every flatten order.
    SmartOrder o = good_smart();
    o.position_size = 0;
    o.quantity = 0;

    const auto r = OrderValidator::validate_smart(o);
    QVERIFY2(r.valid, qPrintable(joined(r)));
    QVERIFY(r.errors.isEmpty());
    QVERIFY(!has_error(r, QStringLiteral("Quantity")));
}

void TstOrderValidator::smart_allows_zero_quantity_for_any_position_size() {
    // Not just the flatten case: quantity is always derivable, so 0 is never an
    // error on a smart order regardless of the target size.
    SmartOrder o = good_smart();
    o.quantity = 0;

    o.position_size = 100;
    QVERIFY(OrderValidator::validate_smart(o).valid);

    o.position_size = -100; // target short
    QVERIFY(OrderValidator::validate_smart(o).valid);

    o.position_size = 0;
    QVERIFY(OrderValidator::validate_smart(o).valid);

    // An explicit quantity is fine too.
    o.quantity = 25;
    QVERIFY(OrderValidator::validate_smart(o).valid);
}

void TstOrderValidator::smart_rejects_negative_quantity() {
    // Only a nonsensical NEGATIVE fallback quantity is an error.
    SmartOrder o = good_smart();
    o.quantity = -1;

    const auto r = OrderValidator::validate_smart(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Quantity cannot be negative")));
    // And specifically NOT the stricter validate() wording.
    QVERIFY(!has_error(r, QStringLiteral("Quantity must be a positive number")));
}

void TstOrderValidator::smart_validates_symbol_and_exchange() {
    SmartOrder o = good_smart();
    o.symbol = QStringLiteral("  ");
    auto r = OrderValidator::validate_smart(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Symbol is required")));

    o = good_smart();
    o.exchange.clear();
    r = OrderValidator::validate_smart(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Exchange is required")));

    o = good_smart();
    o.exchange = QStringLiteral("NOPE");
    r = OrderValidator::validate_smart(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Invalid exchange: NOPE")));
}

void TstOrderValidator::smart_price_and_trigger_rules_match_validate() {
    SmartOrder o = good_smart();

    o.order_type = OrderType::Limit;
    o.price = 0;
    auto r = OrderValidator::validate_smart(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Price is required")));

    o.price = 2500;
    QVERIFY(OrderValidator::validate_smart(o).valid);

    o = good_smart();
    o.order_type = OrderType::StopLoss;
    o.trigger_price = 0;
    r = OrderValidator::validate_smart(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Trigger price is required")));

    o.trigger_price = 2400;
    QVERIFY(OrderValidator::validate_smart(o).valid);

    // A flatten order with a stop type still needs its trigger.
    o = good_smart();
    o.position_size = 0;
    o.quantity = 0;
    o.order_type = OrderType::StopLoss;
    o.trigger_price = 0;
    r = OrderValidator::validate_smart(o);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Trigger price is required")));
    QVERIFY(!has_error(r, QStringLiteral("Quantity")));
}

// ── validate_basket() ────────────────────────────────────────────────────────

void TstOrderValidator::basket_rejects_empty() {
    BasketOrderRequest b;
    const auto r = OrderValidator::validate_basket(b);
    QVERIFY(!r.valid);
    QCOMPARE(r.errors.size(), qsizetype(1));
    QVERIFY(has_error(r, QStringLiteral("Basket has no orders")));
}

void TstOrderValidator::basket_accepts_all_valid_legs() {
    BasketOrderRequest b;
    b.strategy_name = QStringLiteral("pair");
    b.orders.append(good_order());

    UnifiedOrder leg2 = good_order();
    leg2.symbol = QStringLiteral("TCS");
    leg2.side = OrderSide::Sell;
    b.orders.append(leg2);

    const auto r = OrderValidator::validate_basket(b);
    QVERIFY2(r.valid, qPrintable(joined(r)));
    QVERIFY(r.errors.isEmpty());
}

void TstOrderValidator::basket_reports_the_failing_leg_by_index_and_symbol() {
    BasketOrderRequest b;
    b.orders.append(good_order()); // leg 1: fine

    UnifiedOrder bad = good_order();
    bad.symbol = QStringLiteral("TCS");
    bad.quantity = 0; // leg 2: broken
    b.orders.append(bad);

    const auto r = OrderValidator::validate_basket(b);
    QVERIFY(!r.valid);
    QCOMPARE(r.errors.size(), qsizetype(1));
    // 1-based index, then the leg's symbol, then the underlying message.
    QVERIFY2(has_error(r, QStringLiteral("Order 2 (TCS): Quantity must be a positive number")),
             qPrintable(joined(r)));
    QVERIFY(!has_error(r, QStringLiteral("Order 1")));
}

// ── check_risk() ─────────────────────────────────────────────────────────────

using RiskSettings = OrderValidator::RiskSettings;
using RiskContext = OrderValidator::RiskContext;

void TstOrderValidator::risk_defaults_pass_a_normal_order() {
    // Out of the box only the sanity guard is on, and an ordinary order with no
    // known price or funds must sail through it.
    const auto r = OrderValidator::check_risk(good_order(), RiskSettings{}, RiskContext{});
    QVERIFY2(r.valid, qPrintable(joined(r)));
}

void TstOrderValidator::risk_sanity_quantity_ceiling_applies_to_everything() {
    UnifiedOrder o = good_order();
    o.quantity = OrderValidator::kSanityMaxQuantity + 1;

    auto r = OrderValidator::check_risk(o, RiskSettings{}, RiskContext{});
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("beyond any sane single order")));

    // Unlike the configured limits, the ceiling still applies to a reducing order.
    RiskContext reducing;
    reducing.reduces_position = true;
    QVERIFY(!OrderValidator::check_risk(o, RiskSettings{}, reducing).valid);

    // ...and is lifted only when the user turns sanity checks off.
    RiskSettings off;
    off.sanity_checks = false;
    QVERIFY(OrderValidator::check_risk(o, off, RiskContext{}).valid);

    o.quantity = OrderValidator::kSanityMaxQuantity; // boundary is exclusive
    QVERIFY(OrderValidator::check_risk(o, RiskSettings{}, RiskContext{}).valid);
}

void TstOrderValidator::risk_sanity_buying_power_multiple_is_cash_only() {
    UnifiedOrder o = good_order();
    RiskContext ctx;
    ctx.ref_price = 100.0;
    ctx.buying_power = 10'000.0; // 10x multiple -> 100,000 notional cap

    o.quantity = 1'000; // exactly 10x: allowed
    QVERIFY(OrderValidator::check_risk(o, RiskSettings{}, ctx).valid);

    o.quantity = 1'001; // just over: probable fat-finger
    auto r = OrderValidator::check_risk(o, RiskSettings{}, ctx);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("more than 10x the available buying power")));

    // Derivative segments legitimately run 10-50x notional/margin.
    o.exchange = QStringLiteral("NFO");
    QVERIFY(OrderValidator::check_risk(o, RiskSettings{}, ctx).valid);

    // Unknown buying power is not evidence of a typo.
    o.exchange = QStringLiteral("NSE");
    ctx.buying_power = 0.0;
    QVERIFY(OrderValidator::check_risk(o, RiskSettings{}, ctx).valid);

    // Closing out never trips the buying-power test.
    ctx.buying_power = 10'000.0;
    ctx.reduces_position = true;
    QVERIFY(OrderValidator::check_risk(o, RiskSettings{}, ctx).valid);
}

void TstOrderValidator::risk_configured_limits_skip_reducing_and_paper_orders() {
    RiskSettings s;
    s.max_order_quantity = 5;
    s.blocked_symbols << QStringLiteral("RELIANCE");
    s.daily_loss_limit = 100;

    UnifiedOrder o = good_order(); // RELIANCE x10: breaks two limits, P&L unknown

    RiskContext routed;
    QVERIFY(!OrderValidator::check_risk(o, s, routed).valid);

    // A limit must never trap the user in the position it exists to cap.
    RiskContext reducing;
    reducing.reduces_position = true;
    const auto r = OrderValidator::check_risk(o, s, reducing);
    QVERIFY2(r.valid, qPrintable(joined(r)));

    // The local paper simulator is not subject to the user's broker limits.
    RiskContext paper;
    paper.broker_routed = false;
    QVERIFY(OrderValidator::check_risk(o, s, paper).valid);
}

void TstOrderValidator::risk_blocked_symbols_match_plain_and_exchange_qualified() {
    RiskSettings s;
    s.blocked_symbols << QStringLiteral("RELIANCE") << QStringLiteral("NSE:TCS");

    // Order fields are trimmed and upper-cased before matching.
    UnifiedOrder o = good_order();
    o.symbol = QStringLiteral(" reliance ");
    o.exchange = QStringLiteral("nse");
    auto r = OrderValidator::check_risk(o, s, RiskContext{});
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("blocked-symbols list")));

    // An exchange-qualified entry blocks only that venue.
    o.symbol = QStringLiteral("TCS");
    QVERIFY(!OrderValidator::check_risk(o, s, RiskContext{}).valid);
    o.exchange = QStringLiteral("BSE");
    QVERIFY(OrderValidator::check_risk(o, s, RiskContext{}).valid);
}

void TstOrderValidator::risk_max_order_quantity_is_inclusive() {
    RiskSettings s;
    s.max_order_quantity = 100;
    UnifiedOrder o = good_order();

    o.quantity = 100;
    QVERIFY(OrderValidator::check_risk(o, s, RiskContext{}).valid);

    o.quantity = 101;
    const auto r = OrderValidator::check_risk(o, s, RiskContext{});
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("exceeds your max order quantity")));
}

void TstOrderValidator::risk_max_order_value_fails_closed_without_a_price() {
    RiskSettings s;
    s.max_order_value = 50'000;
    UnifiedOrder o = good_order();

    // No reference price: a cap that cannot be evaluated is not a pass.
    auto r = OrderValidator::check_risk(o, s, RiskContext{});
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("no reference price is known")));

    RiskContext ctx;
    ctx.ref_price = 100.0;
    o.quantity = 500; // 50,000: at the cap
    QVERIFY(OrderValidator::check_risk(o, s, ctx).valid);

    o.quantity = 501; // 50,100: over
    r = OrderValidator::check_risk(o, s, ctx);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("exceeds your max order value")));
}

void TstOrderValidator::risk_daily_loss_limit_fails_closed_and_triggers_at_the_limit() {
    RiskSettings s;
    s.daily_loss_limit = 1'000;
    const UnifiedOrder o = good_order();

    // P&L not loaded yet: refuse rather than guess.
    auto r = OrderValidator::check_risk(o, s, RiskContext{});
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("not loaded")));

    RiskContext ctx;
    ctx.day_pnl_known = true;
    ctx.day_pnl = -999.99;
    QVERIFY(OrderValidator::check_risk(o, s, ctx).valid);

    ctx.day_pnl = -1'000.0; // reaching the limit is enough (<=)
    r = OrderValidator::check_risk(o, s, ctx);
    QVERIFY(!r.valid);
    QVERIFY(has_error(r, QStringLiteral("Daily loss limit reached")));
}

void TstOrderValidator::derivative_exchange_classification() {
    QVERIFY(OrderValidator::is_derivative_exchange(QStringLiteral("NFO")));
    QVERIFY(OrderValidator::is_derivative_exchange(QStringLiteral("MCX")));
    QVERIFY(OrderValidator::is_derivative_exchange(QStringLiteral(" cds "))); // trimmed + case-insensitive
    QVERIFY(!OrderValidator::is_derivative_exchange(QStringLiteral("NSE")));
    QVERIFY(!OrderValidator::is_derivative_exchange(QStringLiteral("NASDAQ")));
    QVERIFY(!OrderValidator::is_derivative_exchange(QString()));
}

QTEST_GUILESS_MAIN(TstOrderValidator)
#include "tst_order_validator.moc"
