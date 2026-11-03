// OrderValidator — the stateful half of the pre-trade risk gate: settings I/O
// (SettingsRepository), the thread-safe per-account snapshot, and pre_trade_risk()
// which glues them to the pure check_risk() in OrderValidator.cpp.
//
// Kept out of OrderValidator.cpp so that file stays free of storage / logging
// dependencies and tests/tst_order_validator can link it on its own.

#include "core/logging/Logger.h"
#include "storage/repositories/SettingsRepository.h"
#include "trading/OrderValidator.h"

#include <QHash>
#include <QMutex>
#include <QMutexLocker>

#include <algorithm>
#include <cmath>

namespace fincept::trading {

namespace {

constexpr const char* kOvLog = "OrderValidator";

// Thread-safe snapshot of what the risk gate needs per account. The source caches
// (AccountDataStream) are GUI-thread-only, while orders are placed from executor
// threads, so DataStreamManager mirrors funds / positions / last prices in here.
struct OvAccountSnapshot {
    double available = 0.0;
    bool funds_known = false;
    QVector<BrokerPosition> positions;
    bool positions_known = false;
    QHash<QString, double> ltp; // symbol (upper) -> last price
};

QMutex& ov_snapshot_mutex() {
    static QMutex m;
    return m;
}

QHash<QString, OvAccountSnapshot>& ov_snapshots() {
    static QHash<QString, OvAccountSnapshot> s;
    return s;
}

} // namespace

OrderValidator::RiskSettings OrderValidator::load_risk_settings(bool* ok_out) {
    RiskSettings s;
    auto res = SettingsRepository::instance().get_by_category(QStringLiteral("risk"));
    if (res.is_err()) {
        LOG_ERROR(kOvLog, QString("Risk settings unreadable: %1").arg(QString::fromStdString(res.error())));
        if (ok_out)
            *ok_out = false;
        return s;
    }
    for (const auto& kv : res.value()) {
        const QString v = kv.value.trimmed();
        if (kv.key == QLatin1String("risk.max_order_value"))
            s.max_order_value = std::max(0.0, v.toDouble());
        else if (kv.key == QLatin1String("risk.max_order_quantity"))
            s.max_order_quantity = std::max(0.0, v.toDouble());
        else if (kv.key == QLatin1String("risk.daily_loss_limit"))
            s.daily_loss_limit = std::fabs(v.toDouble());
        else if (kv.key == QLatin1String("risk.sanity_checks"))
            s.sanity_checks = (v != QLatin1String("0"));
        else if (kv.key == QLatin1String("risk.blocked_symbols")) {
            for (const QString& part : v.split(QLatin1Char(','), Qt::SkipEmptyParts)) {
                const QString t = part.trimmed().toUpper();
                if (!t.isEmpty())
                    s.blocked_symbols << t;
            }
        }
    }
    if (!std::isfinite(s.max_order_value))
        s.max_order_value = 0.0;
    if (!std::isfinite(s.max_order_quantity))
        s.max_order_quantity = 0.0;
    if (!std::isfinite(s.daily_loss_limit))
        s.daily_loss_limit = 0.0;
    if (ok_out)
        *ok_out = true;
    return s;
}

bool OrderValidator::save_risk_settings(const RiskSettings& s) {
    auto& repo = SettingsRepository::instance();
    const QString cat = QStringLiteral("risk");
    bool ok = repo.set("risk.max_order_value", QString::number(s.max_order_value, 'f', 2), cat).is_ok();
    ok = repo.set("risk.max_order_quantity", QString::number(s.max_order_quantity, 'f', 0), cat).is_ok() && ok;
    ok = repo.set("risk.daily_loss_limit", QString::number(s.daily_loss_limit, 'f', 2), cat).is_ok() && ok;
    ok = repo.set("risk.sanity_checks", s.sanity_checks ? "1" : "0", cat).is_ok() && ok;
    ok = repo.set("risk.blocked_symbols", s.blocked_symbols.join(QLatin1Char(',')), cat).is_ok() && ok;
    LOG_INFO(kOvLog, QString("Risk limits saved: max value %1, max qty %2, daily loss %3, %4 blocked, sanity %5")
                         .arg(s.max_order_value)
                         .arg(s.max_order_quantity)
                         .arg(s.daily_loss_limit)
                         .arg(s.blocked_symbols.size())
                         .arg(s.sanity_checks ? "on" : "off"));
    return ok;
}

OrderValidator::ValidationResult OrderValidator::pre_trade_risk(const QString& account_id, const UnifiedOrder& order,
                                                                bool broker_routed) {
    bool settings_ok = true;
    const RiskSettings s = load_risk_settings(&settings_ok);
    if (!settings_ok) {
        ValidationResult r;
        r.valid = false;
        r.errors << QStringLiteral("Risk settings could not be read — order refused (fail closed)");
        return r;
    }

    RiskContext ctx;
    ctx.broker_routed = broker_routed;
    const QString sym = order.symbol.trimmed().toUpper();
    const QString exch = order.exchange.trimmed().toUpper();
    if ((order.order_type == OrderType::Limit || order.order_type == OrderType::StopLossLimit) && order.price > 0.0)
        ctx.ref_price = order.price;
    else if (order.order_type == OrderType::StopLoss && order.stop_price > 0.0)
        ctx.ref_price = order.stop_price;
    else if (order.price > 0.0)
        ctx.ref_price = order.price; // paper market orders carry the LTP here

    {
        QMutexLocker lock(&ov_snapshot_mutex());
        const auto it = ov_snapshots().constFind(account_id);
        if (it != ov_snapshots().constEnd()) {
            const OvAccountSnapshot& snap = it.value();
            if (snap.funds_known)
                ctx.buying_power = snap.available;
            if (ctx.ref_price <= 0.0)
                ctx.ref_price = snap.ltp.value(sym, 0.0);
            if (snap.positions_known) {
                ctx.day_pnl_known = true;
                for (const auto& p : snap.positions) {
                    ctx.day_pnl += p.pnl;
                    const bool same_instrument = p.symbol.trimmed().toUpper() == sym &&
                                                 (exch.isEmpty() || p.exchange.isEmpty() ||
                                                  p.exchange.trimmed().toUpper() == exch);
                    if (!same_instrument || p.quantity == 0.0)
                        continue;
                    if (ctx.ref_price <= 0.0 && p.ltp > 0.0)
                        ctx.ref_price = p.ltp;
                    // Reduces only when it is the closing side, in the SAME product
                    // (Indian brokers net per product) and no larger than the position.
                    if (closing_side_for_position(p.side, p.quantity) == order.side &&
                        product_from_broker_str(p.product_type) == order.product_type &&
                        order.quantity <= std::abs(p.quantity) + 1e-9)
                        ctx.reduces_position = true;
                }
            }
        }
    }

    ValidationResult r = check_risk(order, s, ctx);
    if (!r.valid)
        LOG_WARN(kOvLog, QString("Pre-trade risk gate refused %1 %2 x%3 on %4: %5")
                             .arg(QString::fromLatin1(order_side_str(order.side)), order.symbol)
                             .arg(order.quantity)
                             .arg(account_id, r.errors.join("; ")));
    return r;
}

void OrderValidator::note_funds(const QString& account_id, double available_balance) {
    if (account_id.isEmpty() || !std::isfinite(available_balance))
        return;
    QMutexLocker lock(&ov_snapshot_mutex());
    auto& snap = ov_snapshots()[account_id];
    snap.available = available_balance;
    snap.funds_known = true;
}

void OrderValidator::note_positions(const QString& account_id, const QVector<BrokerPosition>& positions) {
    if (account_id.isEmpty())
        return;
    QMutexLocker lock(&ov_snapshot_mutex());
    auto& snap = ov_snapshots()[account_id];
    snap.positions = positions;
    snap.positions_known = true;
}

void OrderValidator::note_quote(const QString& account_id, const QString& symbol, double ltp) {
    if (account_id.isEmpty() || symbol.isEmpty() || !(ltp > 0.0) || !std::isfinite(ltp))
        return;
    QMutexLocker lock(&ov_snapshot_mutex());
    ov_snapshots()[account_id].ltp.insert(symbol.trimmed().toUpper(), ltp);
}

} // namespace fincept::trading
