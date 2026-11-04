// src/algo_engine/StreamingOperand.cpp
#include "algo_engine/StreamingOperand.h"

#include "algo_engine/StreamingIndicators.h"

#include <initializer_list>

namespace fincept::algo {

namespace {

// Names below carry an so_ prefix: unity builds merge this file's anonymous
// namespace with its neighbours' (IndicatorEngine, SeriesOperandSource, ...).

// IndicatorEngine::compute's parameter parsing — same keys, defaults and clamp.
int so_period(const QJsonObject& p, const char* key, int fallback) {
    const int v = p.value(QLatin1String(key)).toInt(fallback);
    return v >= 1 ? v : 1;
}

int so_index_of(const QString& field, std::initializer_list<const char*> names) {
    int i = 0;
    for (const char* n : names) {
        if (field == QLatin1String(n))
            return i;
        ++i;
    }
    return -1;
}

// `Feed` folds a candle into `Ind`; `Read` extracts the operand's field. Both are
// stateless or capture only values, so the whole operand copies cleanly.
template <class Ind, class Feed, class Read>
class so_Operand final : public StreamingOperand {
  public:
    so_Operand(const char* warmup_error, Ind ind, Feed feed, Read read)
        : StreamingOperand(QString::fromLatin1(warmup_error)), ind_(std::move(ind)), feed_(feed), read_(read) {}

    void update(const OhlcvCandle& bar) override { feed_(ind_, bar); }
    double value() const override { return read_(ind_); }
    std::unique_ptr<StreamingOperand> clone() const override { return std::make_unique<so_Operand>(*this); }

  private:
    Ind ind_;
    Feed feed_;
    Read read_;
};

template <class Ind, class Feed, class Read>
std::unique_ptr<StreamingOperand> so_make(const char* warmup_error, Ind ind, Feed feed, Read read) {
    return std::make_unique<so_Operand<Ind, Feed, Read>>(warmup_error, std::move(ind), feed, read);
}

// The last bar's price attribute; NaN until the first bar.
struct so_Latest {
    double v = streaming::kNaN;
};

} // namespace

std::unique_ptr<StreamingOperand> StreamingOperand::create(const QString& ind, const QJsonObject& p,
                                                           const QString& field) {
    const auto feed_close = [](auto& s, const OhlcvCandle& k) { s.update(k.close); };
    const auto feed_hlc = [](auto& s, const OhlcvCandle& k) { s.update(k.high, k.low, k.close); };
    const auto read_value = [](const auto& s) { return s.value(); };
    const int period = so_period(p, "period", 14);

    if (field == QLatin1String("value")) {
        double OhlcvCandle::*attr = nullptr;
        if (ind == "CLOSE")
            attr = &OhlcvCandle::close;
        else if (ind == "OPEN")
            attr = &OhlcvCandle::open;
        else if (ind == "HIGH")
            attr = &OhlcvCandle::high;
        else if (ind == "LOW")
            attr = &OhlcvCandle::low;
        else if (ind == "VOLUME")
            attr = &OhlcvCandle::volume;
        if (attr) {
            return so_make(
                "Insufficient data", so_Latest{}, [attr](so_Latest& s, const OhlcvCandle& k) { s.v = k.*attr; },
                [](const so_Latest& s) { return s.v; });
        }
        if (ind == "SMA")
            return so_make("Insufficient data for SMA", streaming::Sma(period), feed_close, read_value);
        if (ind == "EMA")
            return so_make("Insufficient data for EMA", streaming::Ema(period), feed_close, read_value);
        if (ind == "RSI")
            return so_make("Insufficient data for RSI", streaming::Rsi(period), feed_close, read_value);
        if (ind == "ATR")
            return so_make("Insufficient data for ATR", streaming::Atr(period), feed_hlc, read_value);
        if (ind == "WILLIAMS_R")
            return so_make("Insufficient data for Williams %R", streaming::WilliamsR(period), feed_hlc, read_value);
        if (ind == "ZSCORE")
            return so_make("Insufficient data for ZSCORE", streaming::ZScore(period), feed_close, read_value);
        if (ind == "REALIZED_VOL")
            return so_make(
                "Insufficient data for REALIZED_VOL",
                streaming::RealizedVolatility(period, p.value(QLatin1String("periods_per_year")).toDouble(252.0)),
                feed_close, read_value);
        return nullptr;
    }

    if (ind == "MACD") {
        const int which = so_index_of(field, {"line", "signal_line", "histogram"});
        if (which < 0)
            return nullptr;
        return so_make(
            "Insufficient data for MACD",
            streaming::Macd(so_period(p, "fast", 12), so_period(p, "slow", 26), so_period(p, "signal", 9)), feed_close,
            [which](const streaming::Macd& m) {
                return which == 0 ? m.line() : which == 1 ? m.signal() : m.histogram();
            });
    }
    if (ind == "BOLLINGER") {
        const int which = so_index_of(field, {"upper", "middle", "lower", "width", "pct_b"});
        if (which < 0)
            return nullptr;
        return so_make("Insufficient data for Bollinger",
                       streaming::Bollinger(period, p.value(QLatin1String("std_dev")).toDouble(2.0)), feed_close,
                       [which](const streaming::Bollinger& b) {
                           const auto bands = b.bands();
                           switch (which) {
                               case 0:
                                   return bands.upper;
                               case 1:
                                   return bands.middle;
                               case 2:
                                   return bands.lower;
                               case 3:
                                   return bands.width;
                               default:
                                   return bands.pct_b;
                           }
                       });
    }
    if (ind == "DONCHIAN") {
        const int which = so_index_of(field, {"upper", "lower"});
        if (which < 0)
            return nullptr;
        return so_make(
            "Insufficient data for Donchian", streaming::Donchian(period),
            [](streaming::Donchian& d, const OhlcvCandle& k) { d.update(k.high, k.low); },
            [which](const streaming::Donchian& d) { return which == 0 ? d.upper() : d.lower(); });
    }
    if (ind == "STOCHASTIC") {
        const int which = so_index_of(field, {"k", "d"});
        if (which < 0)
            return nullptr;
        return so_make("Insufficient data for Stochastic",
                       streaming::Stochastic(so_period(p, "k_period", 14), so_period(p, "d_period", 3)), feed_hlc,
                       [which](const streaming::Stochastic& s) { return which == 0 ? s.k() : s.d(); });
    }
    return nullptr;
}

} // namespace fincept::algo
