// src/algo_engine/OperandSource.cpp
#include "algo_engine/OperandSource.h"

#include "algo_engine/IndicatorEngine.h"

#include <cmath>
#include <limits>

namespace fincept::algo {

double WindowOperandSource::value(const QString& indicator, const QJsonObject& params, const QString& field,
                                  int offset, QString* error) {
    constexpr double nan = std::numeric_limits<double>::quiet_NaN();
    const QVector<OhlcvCandle>& candles = *candles_;
    if (offset < 0)
        offset = 0;
    QVector<OhlcvCandle> window = candles;
    if (offset > 0) {
        if (offset >= candles.size()) {
            if (error)
                *error = QStringLiteral("insufficient data for offset %1").arg(offset);
            return nan;
        }
        window = candles.mid(0, candles.size() - offset);
    }
    auto r = IndicatorEngine::compute(indicator, window, params, field);
    if (!r.valid) {
        if (error)
            *error = r.error;
        return nan;
    }
    // Single choke point for every operand: a non-finite indicator value is a data
    // defect, not a tradeable level. Normalise it to NaN + an error so no operator
    // downstream can compare against ±inf.
    const double v = r.current.value(field, nan);
    if (!std::isfinite(v)) {
        if (error)
            *error = QStringLiteral("%1 produced a non-finite value").arg(indicator);
        return nan;
    }
    return v;
}

} // namespace fincept::algo
