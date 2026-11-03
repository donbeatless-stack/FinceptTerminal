// Pure half of OrderValidator: field validation and check_risk(). Settings I/O,
// the account snapshot and pre_trade_risk() live in OrderValidatorRiskGate.cpp —
// keep storage / logging includes out of this file (tests/tst_order_validator
// links it standalone).

#include "trading/OrderValidator.h"

#include <QLocale>

#include <cmath>

namespace fincept::trading {

namespace {

QString ov_money(double v) {
    return QLocale(QLocale::English).toString(v, 'f', 2);
}

} // namespace

const QSet<QString>& OrderValidator::valid_exchanges() {
    static const QSet<QString> kExchanges = {
        // India
        "NSE", "BSE", "NFO", "BFO", "MCX", "CDS", "BCD", "NCDEX", "NSE_INDEX", "BSE_INDEX", "MCX_INDEX",
        // US / global
        "NYSE", "NASDAQ", "AMEX", "ARCA", "BATS", "CBOE", "LSE", "TSX", "XETRA", "EURONEXT", "XNYS", "XNAS",
        // crypto / forex
        "CRYPTO", "FOREX"};
    return kExchanges;
}

bool OrderValidator::is_valid_exchange(const QString& exchange) {
    return valid_exchanges().contains(exchange.toUpper());
}

bool OrderValidator::requires_price(OrderType type) {
    return type == OrderType::Limit || type == OrderType::StopLossLimit;
}

bool OrderValidator::requires_trigger_price(OrderType type) {
    return type == OrderType::StopLoss || type == OrderType::StopLossLimit;
}

OrderValidator::ValidationResult OrderValidator::validate(const UnifiedOrder& order) {
    ValidationResult r;

    if (order.symbol.trimmed().isEmpty())
        r.errors.append("Symbol is required");
    if (order.exchange.trimmed().isEmpty())
        r.errors.append("Exchange is required");
    else if (!is_valid_exchange(order.exchange))
        r.errors.append("Invalid exchange: " + order.exchange);

    // NaN compares false against everything, so a NaN quantity used to slip past
    // "quantity <= 0" and reach the broker serializer. Reject non-finite numbers.
    if (!std::isfinite(order.quantity) || order.quantity <= 0)
        r.errors.append("Quantity must be a positive number");

    if (!std::isfinite(order.price) || order.price < 0 || !std::isfinite(order.stop_price) || order.stop_price < 0)
        r.errors.append("Price / trigger must be finite and not negative");

    if (requires_price(order.order_type) && order.price <= 0)
        r.errors.append("Price is required for LIMIT / SL-Limit orders");

    if (requires_trigger_price(order.order_type) && order.stop_price <= 0)
        r.errors.append("Trigger price is required for SL / SL-M orders");

    r.valid = r.errors.isEmpty();
    return r;
}

// ── Pre-trade risk gate ───────────────────────────────────────────────────────

bool OrderValidator::is_derivative_exchange(const QString& exchange) {
    static const QSet<QString> kDerivative = {"NFO", "BFO", "MCX", "CDS", "BCD", "NCDEX", "MCX_INDEX"};
    return kDerivative.contains(exchange.trimmed().toUpper());
}

OrderValidator::ValidationResult OrderValidator::check_risk(const UnifiedOrder& order, const RiskSettings& s,
                                                            const RiskContext& ctx) {
    ValidationResult r;
    const QString sym = order.symbol.trimmed().toUpper();
    const QString exch = order.exchange.trimmed().toUpper();
    const double notional = ctx.ref_price > 0.0 ? ctx.ref_price * order.quantity : 0.0;

    // 1. Absurd-order guard — on unless the user switched it off.
    if (s.sanity_checks) {
        if (order.quantity > kSanityMaxQuantity)
            r.errors.append(QString("Quantity %1 is beyond any sane single order (> %2) — rejected as a probable typo")
                                .arg(QLocale(QLocale::English).toString(order.quantity, 'f', 0))
                                .arg(QLocale(QLocale::English).toString(kSanityMaxQuantity, 'f', 0)));
        if (!ctx.reduces_position && !is_derivative_exchange(exch) && notional > 0.0 && ctx.buying_power > 0.0 &&
            notional > kSanityBuyingPowerMultiple * ctx.buying_power)
            r.errors.append(QString("Order value %1 is more than %2x the available buying power %3 — rejected as a "
                                    "probable typo (extra zero?)")
                                .arg(ov_money(notional))
                                .arg(kSanityBuyingPowerMultiple, 0, 'f', 0)
                                .arg(ov_money(ctx.buying_power)));
    }

    // 2. Configured limits apply to broker-routed orders that ADD risk. Closing /
    //    reducing an existing position is always allowed — a limit must never trap
    //    the user in the position it exists to cap.
    if (ctx.broker_routed && !ctx.reduces_position) {
        if (!s.blocked_symbols.isEmpty() &&
            (s.blocked_symbols.contains(sym) || (!exch.isEmpty() && s.blocked_symbols.contains(exch + ":" + sym))))
            r.errors.append(QString("%1 is on your blocked-symbols list (Risk limits)").arg(order.symbol));

        if (s.max_order_quantity > 0.0 && order.quantity > s.max_order_quantity)
            r.errors.append(QString("Quantity %1 exceeds your max order quantity %2 (Risk limits)")
                                .arg(order.quantity)
                                .arg(s.max_order_quantity));

        if (s.max_order_value > 0.0) {
            if (notional <= 0.0)
                // Fail closed: a configured cap that cannot be evaluated is not a pass.
                r.errors.append(QString("Max order value %1 is set but no reference price is known for this order — "
                                        "use a LIMIT order or wait for quotes to load")
                                    .arg(ov_money(s.max_order_value)));
            else if (notional > s.max_order_value)
                r.errors.append(QString("Order value %1 exceeds your max order value %2 (Risk limits)")
                                    .arg(ov_money(notional), ov_money(s.max_order_value)));
        }

        if (s.daily_loss_limit > 0.0) {
            if (!ctx.day_pnl_known)
                r.errors.append(QString("Daily loss limit %1 is set but today's P&L for this account is not loaded "
                                        "yet — open the account in Equity Trading, then retry")
                                    .arg(ov_money(s.daily_loss_limit)));
            else if (ctx.day_pnl <= -s.daily_loss_limit)
                r.errors.append(QString("Daily loss limit reached: today's P&L %1 <= -%2. Only orders that reduce an "
                                        "open position are allowed until tomorrow (Risk limits)")
                                    .arg(ov_money(ctx.day_pnl), ov_money(s.daily_loss_limit)));
        }
    }

    r.valid = r.errors.isEmpty();
    return r;
}

OrderValidator::ValidationResult OrderValidator::validate_smart(const SmartOrder& order) {
    ValidationResult r;

    if (order.symbol.trimmed().isEmpty())
        r.errors.append("Symbol is required");
    if (order.exchange.trimmed().isEmpty())
        r.errors.append("Exchange is required");
    else if (!is_valid_exchange(order.exchange))
        r.errors.append("Invalid exchange: " + order.exchange);

    // position_size is the TARGET net position, and 0 is a legitimate target —
    // "flatten": the engine reads the live book and derives the quantity itself.
    // An explicit 0 is indistinguishable from an unset one, so requiring a
    // fallback quantity here would reject every flatten order. The engine already
    // reports the genuinely-empty case (no position AND no quantity) as a benign
    // "no action needed", so only a nonsensical negative fallback is an error.
    if (order.quantity < 0)
        r.errors.append("Quantity cannot be negative");

    if (requires_price(order.order_type) && order.price <= 0)
        r.errors.append("Price is required for LIMIT / SL-Limit orders");

    if (requires_trigger_price(order.order_type) && order.trigger_price <= 0)
        r.errors.append("Trigger price is required for SL / SL-M orders");

    r.valid = r.errors.isEmpty();
    return r;
}

OrderValidator::ValidationResult OrderValidator::validate_basket(const BasketOrderRequest& basket) {
    ValidationResult r;

    if (basket.orders.isEmpty()) {
        r.errors.append("Basket has no orders");
        r.valid = false;
        return r;
    }

    for (int i = 0; i < basket.orders.size(); ++i) {
        auto leg = validate(basket.orders[i]);
        if (!leg.valid) {
            for (const auto& e : leg.errors)
                r.errors.append(QString("Order %1 (%2): %3").arg(i + 1).arg(basket.orders[i].symbol, e));
        }
    }

    r.valid = r.errors.isEmpty();
    return r;
}

} // namespace fincept::trading
