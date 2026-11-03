// Benchmark for RealtimeScanRunner's sweep: the cost of evaluating one strategy
// across a symbol universe, and how far live values drift from full-history
// indicator values — the old way and the new way.
//
//   old = what RealtimeScanRunner did: per symbol, the last required_bars + 50
//         closed candles (min 60) plus synthetic bars for the previous and the
//         current tick; conditions parsed and every indicator recomputed.
//   new = what it does now: CompiledConditions over a LiveOperandSource warmed
//         with the whole history; a sweep applies only the two tick bars.
//
// Not registered with ctest: timings are machine-dependent. Usage:
//   bench_realtime_scan [symbols]        (default 500)

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/ConditionEvaluator.h"
#include "algo_engine/IndicatorEngine.h"
#include "algo_engine/LiveOperandSource.h"

#include <QJsonArray>
#include <QJsonObject>
#include <QString>
#include <QVector>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <vector>

using fincept::algo::CompiledConditions;
using fincept::algo::ConditionEvaluator;
using fincept::algo::IndicatorEngine;
using fincept::algo::LiveOperandSource;
using fincept::algo::OhlcvCandle;
using fincept::algo::OperandRef;

namespace {

constexpr int kHistory = 2'000; // bars fetched at warm-up (lookback_days worth)

QVector<OhlcvCandle> make_walk(int n, std::uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::normal_distribution<double> ret(0.0002, 0.012);
    std::uniform_real_distribution<double> wick(0.0, 0.004);
    QVector<OhlcvCandle> out;
    out.reserve(n);
    double close = 50.0 + static_cast<double>(seed % 400);
    for (int i = 0; i < n; ++i) {
        OhlcvCandle c;
        c.open_time = c.close_time = i;
        c.open = close;
        close *= std::exp(ret(rng));
        c.close = close;
        c.high = std::max(c.open, c.close) * (1.0 + wick(rng));
        c.low = std::min(c.open, c.close) * (1.0 - wick(rng));
        c.volume = 1000;
        c.is_closed = true;
        out.append(c);
    }
    return out;
}

OhlcvCandle tick_bar(double price) {
    OhlcvCandle c;
    c.open = c.high = c.low = c.close = price;
    c.is_closed = true;
    return c;
}

QJsonObject vs_value(const char* ind, const QJsonObject& params, const char* field, const char* op, double value) {
    return {{"indicator", ind}, {"params", params}, {"field", field}, {"operator", op}, {"value", value}};
}

QJsonObject vs_indicator(const char* ind, const QJsonObject& params, const char* field, const char* op,
                         const char* cmp, const QJsonObject& cmp_params, const char* cmp_field) {
    return {{"indicator", ind},
            {"params", params},
            {"field", field},
            {"operator", op},
            {"compare_mode", "indicator"},
            {"compare_indicator", cmp},
            {"compare_params", cmp_params},
            {"compare_field", cmp_field}};
}

struct Strategy {
    const char* name;
    QJsonArray conditions;
    QString logic;
};

// RealtimeScanRunner::required_bars, copied: linking RealtimeScanRunner.cpp would
// drag in the live data stream stack. Only "period" keys count — MACD and
// Stochastic, which have none, get the 60-bar floor.
int required_bars(const QJsonArray& conditions) {
    int max_need = 2;
    for (const auto& v : conditions) {
        const QJsonObject o = v.toObject();
        const int period = o.value("params").toObject().value("period").toInt(0);
        const int cperiod = o.value("compare_params").toObject().value("period").toInt(0);
        const int offset = std::max(o.value("offset").toInt(0), o.value("compare_offset").toInt(0));
        max_need = std::max(max_need, std::max(period, cperiod) + offset + 2);
    }
    return max_need;
}

double ms(std::chrono::steady_clock::duration d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

} // namespace

int main(int argc, char** argv) {
    const int symbols = argc > 1 ? std::max(1, std::atoi(argv[1])) : 500;
    using clock = std::chrono::steady_clock;

    std::vector<QVector<OhlcvCandle>> history;
    history.reserve(static_cast<std::size_t>(symbols));
    for (int s = 0; s < symbols; ++s)
        history.push_back(make_walk(kHistory, 1000 + static_cast<std::uint64_t>(s)));

    const std::vector<Strategy> strategies = {
        {"CLOSE crosses EMA(200)",
         {vs_indicator("CLOSE", {}, "value", "crosses_above", "EMA", {{"period", 200}}, "value")},
         "AND"},
        {"RSI(14)<30 & CLOSE>SMA(50)",
         {vs_value("RSI", {{"period", 14}}, "value", "<", 30),
          vs_indicator("CLOSE", {}, "value", ">", "SMA", {{"period", 50}}, "value")},
         "AND"},
        {"MACD hist rising & %K<25",
         {vs_value("MACD", {}, "histogram", "rising", 0), vs_value("STOCHASTIC", {}, "k", "<", 25)},
         "AND"},
    };

    std::printf("RealtimeScanRunner sweep: %d symbols, %d bars of warm-up history each\n\n", symbols, kHistory);
    std::printf("%-28s %7s %9s %9s %8s %14s %9s\n", "strategy", "buffer", "old ms", "new ms", "speedup",
                "diff verdicts", "warm ms");

    for (const Strategy& st : strategies) {
        const int buffer = std::max(required_bars(st.conditions) + 50, 60);
        std::vector<double> prev_tick(history.size()), cur_tick(history.size());
        for (std::size_t s = 0; s < history.size(); ++s) {
            prev_tick[s] = history[s].last().close * 0.999;
            cur_tick[s] = history[s].last().close * 1.001;
        }

        // Old: the candle buffer plus the tick bars, everything recomputed.
        std::vector<QVector<OhlcvCandle>> closed;
        closed.reserve(history.size());
        for (const auto& h : history)
            closed.push_back(h.mid(h.size() - buffer));
        std::vector<char> old_verdict(history.size()), new_verdict(history.size());
        const auto o0 = clock::now();
        for (std::size_t s = 0; s < closed.size(); ++s) {
            QVector<OhlcvCandle> window = closed[s];
            window.append(tick_bar(prev_tick[s]));
            window.append(tick_bar(cur_tick[s]));
            old_verdict[s] = ConditionEvaluator::evaluate_group(st.conditions, st.logic, window).triggered;
        }
        const auto old_sweep = clock::now() - o0;

        // New: per-symbol state built once from the whole history (warm-up), then
        // a sweep that only applies the tick bars.
        const CompiledConditions compiled(st.conditions, st.logic);
        const std::vector<OperandRef> refs = compiled.operands();
        std::vector<std::unique_ptr<LiveOperandSource>> live;
        live.reserve(history.size());
        const auto w0 = clock::now();
        for (const auto& h : history) {
            live.push_back(std::make_unique<LiveOperandSource>(refs));
            live.back()->reset(h);
        }
        const auto warm = clock::now() - w0;

        const auto n0 = clock::now();
        for (std::size_t s = 0; s < live.size(); ++s) {
            LiveOperandSource& ops = *live[s];
            ops.set_ticks({tick_bar(prev_tick[s]), tick_bar(cur_tick[s])});
            new_verdict[s] = compiled.evaluate(ops).triggered;
        }
        const auto new_sweep = clock::now() - n0;

        int diff = 0;
        for (std::size_t s = 0; s < history.size(); ++s)
            diff += old_verdict[s] != new_verdict[s] ? 1 : 0;
        std::printf("%-28s %7d %9.2f %9.2f %7.0fx %14d %9.1f\n", st.name, buffer, ms(old_sweep), ms(new_sweep),
                    ms(old_sweep) / ms(new_sweep), diff, ms(warm));
    }

    // Error at the last bar against the full-history value, across the universe.
    std::printf("\nError vs full-history value (median / max over symbols; %% of price, RSI in points):\n");
    struct Probe {
        const char* label;
        const char* ind;
        QJsonObject params;
        const char* field;
    };
    const std::vector<Probe> probes = {
        {"EMA(50)", "EMA", {{"period", 50}}, "value"},
        {"EMA(200)", "EMA", {{"period", 200}}, "value"},
        {"RSI(14)", "RSI", {{"period", 14}}, "value"},
        {"MACD(12,26,9) line", "MACD", {}, "line"},
    };
    for (const Probe& p : probes) {
        const int buffer = std::max(p.params.value("period").toInt(0) + 2 + 50, 60);
        const bool rsi = QString(p.ind) == "RSI";
        const std::vector<OperandRef> refs{{p.ind, p.params, p.field, 0}};
        std::vector<double> old_errs, new_errs;
        for (const auto& h : history) {
            const double full = IndicatorEngine::compute(p.ind, h, p.params, p.field).current.value(p.field);
            const double old_live =
                IndicatorEngine::compute(p.ind, h.mid(h.size() - buffer), p.params, p.field).current.value(p.field);
            LiveOperandSource live(refs);
            live.reset(h);
            live.set_ticks({});
            const double new_live = live.value(p.ind, p.params, p.field, 0, nullptr);
            const double scale = rsi ? 1.0 : h.last().close / 100.0;
            old_errs.push_back(std::abs(full - old_live) / scale);
            new_errs.push_back(std::abs(full - new_live) / scale);
        }
        std::sort(old_errs.begin(), old_errs.end());
        std::sort(new_errs.begin(), new_errs.end());
        std::printf("  %-20s old (buffer %3d): median %8.4f  max %8.4f    new: median %8.4f  max %8.4f\n", p.label,
                    buffer, old_errs[old_errs.size() / 2], old_errs.back(), new_errs[new_errs.size() / 2],
                    new_errs.back());
    }
    return 0;
}
