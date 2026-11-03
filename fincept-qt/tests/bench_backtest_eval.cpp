// Benchmark + parity check for BacktestEngine's condition evaluation: the old
// path (parse the condition JSON and copy the trailing 500 candles each bar,
// recompute every operand — WindowOperandSource) against the new one
// (CompiledConditions over a SeriesOperandSource), with the intermediate step
// (series operands, JSON still parsed per bar) shown separately.
//
// For each strategy the entry and exit groups are evaluated at EVERY bar the old
// and the new way. BacktestEngine's fills depend on nothing but these verdicts,
// so equal verdicts at every bar mean identical trades; "diff bars" counts the
// bars where they differ, and "verdicts#" fingerprints the old path's verdicts
// so a change to the evaluator itself shows up as a changed checksum. The real
// BacktestEngine::run is then timed end to end.
//
// Not registered with ctest: timings are machine-dependent. Usage:
//   bench_backtest_eval [bars]        (default 20000)

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/BacktestEngine.h"
#include "algo_engine/ConditionEvaluator.h"
#include "algo_engine/SeriesOperandSource.h"
#include "core/logging/Logger.h"

#include <QCoreApplication>
#include <QJsonArray>
#include <QJsonObject>
#include <QString>
#include <QVector>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <vector>

using fincept::algo::BacktestEngine;
using fincept::algo::ConditionEvaluator;
using fincept::algo::OhlcvCandle;
using fincept::algo::SeriesOperandSource;

namespace {

constexpr int kEvalWindow = 500; // BacktestEngine.cpp
constexpr int kWarmupBars = 50;  // BacktestEngine.cpp

QVector<OhlcvCandle> make_walk(int n) {
    std::mt19937_64 rng(2026);
    std::normal_distribution<double> ret(0.0003, 0.011);
    std::uniform_real_distribution<double> wick(0.0, 0.005);
    std::uniform_real_distribution<double> vol(1'000.0, 9'000.0);
    QVector<OhlcvCandle> out;
    out.reserve(n);
    double close = 100.0;
    for (int i = 0; i < n; ++i) {
        OhlcvCandle c;
        // Daily bars from 2000-01-03, so the engine's monthly-return table is sane.
        c.open_time = (946857600LL + 86400LL * i) * 1000;
        c.close_time = c.open_time + 86'399'000;
        c.open = close;
        close *= std::exp(ret(rng));
        c.close = close;
        c.high = std::max(c.open, c.close) * (1.0 + wick(rng));
        c.low = std::min(c.open, c.close) * (1.0 - wick(rng));
        c.volume = vol(rng);
        c.is_closed = true;
        out.append(c);
    }
    return out;
}

QJsonObject vs_value(const char* ind, const QJsonObject& params, const char* field, const char* op, double value) {
    return {{"indicator", ind}, {"params", params}, {"field", field}, {"operator", op}, {"value", value}};
}

QJsonObject vs_indicator(const char* ind, const QJsonObject& params, const char* field, const char* op,
                         const char* cmp, const QJsonObject& cmp_params, const char* cmp_field) {
    return {{"indicator", ind},     {"params", params},         {"field", field},
            {"operator", op},       {"compare_mode", "indicator"}, {"compare_indicator", cmp},
            {"compare_params", cmp_params}, {"compare_field", cmp_field}};
}

struct Strategy {
    const char* name;
    QJsonArray entry;
    QString entry_logic;
    QJsonArray exit;
    QString exit_logic;
};

std::vector<Strategy> strategies() {
    const QJsonObject ema12{{"period", 12}}, ema26{{"period", 26}}, ema200{{"period", 200}};
    const QJsonObject rsi14{{"period", 14}}, sma200{{"period", 200}}, bb{{"period", 20}, {"std_dev", 2.0}};
    return {
        {"EMA 12/26 crossover",
         {vs_indicator("EMA", ema12, "value", "crosses_above", "EMA", ema26, "value")},
         "AND",
         {vs_indicator("EMA", ema12, "value", "crosses_below", "EMA", ema26, "value")},
         "AND"},
        {"RSI<30 + SMA200 trend",
         {vs_value("RSI", rsi14, "value", "<", 30), vs_indicator("CLOSE", {}, "value", ">", "SMA", sma200, "value")},
         "AND",
         {vs_value("RSI", rsi14, "value", ">", 55)},
         "AND"},
        {"MACD hist + Stochastic",
         {vs_value("MACD", {}, "histogram", "rising", 0), vs_value("STOCHASTIC", {}, "k", "<", 25)},
         "AND",
         {vs_value("STOCHASTIC", {}, "k", ">", 80)},
         "AND"},
        {"Bollinger / Williams %R",
         {vs_indicator("CLOSE", {}, "value", "crosses_above", "BOLLINGER", bb, "upper"),
          vs_value("WILLIAMS_R", {{"period", 14}}, "value", ">", -5)},
         "OR",
         {vs_indicator("CLOSE", {}, "value", "<", "BOLLINGER", bb, "middle")},
         "AND"},
        {"EMA(200) crossover",
         {vs_indicator("CLOSE", {}, "value", "crosses_above", "EMA", ema200, "value")},
         "AND",
         {vs_indicator("CLOSE", {}, "value", "crosses_below", "EMA", ema200, "value")},
         "AND"},
        {"CCI + ADX (no stream)",
         {vs_value("CCI", {{"period", 20}}, "value", ">", 100), vs_value("ADX", {{"period", 14}}, "value", ">", 20)},
         "AND",
         {vs_value("CCI", {{"period", 20}}, "value", "<", 0)},
         "AND"},
    };
}

double ms(std::chrono::steady_clock::duration d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

// Answers every operand instantly, so timing evaluation through it measures the
// evaluator's own per-bar cost (condition parsing, operators, result records):
// the floor no operand source can go below.
class ConstantOperands final : public fincept::algo::OperandSource {
  public:
    double value(const QString&, const QJsonObject&, const QString&, int offset, QString*) override {
        return 50.0 + offset;
    }
};

} // namespace

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    fincept::Logger::instance().set_level(fincept::LogLevel::Warn); // keep run()'s info lines out of the table

    const int bars = argc > 1 ? std::max(kEvalWindow * 2, std::atoi(argv[1])) : 20'000;
    const QVector<OhlcvCandle> candles = make_walk(bars);
    using clock = std::chrono::steady_clock;

    std::printf("BacktestEngine condition evaluation: %d bars, entry+exit groups evaluated at every bar\n", bars);
    std::printf("  old      = JSON parsed per bar, operands recomputed on a 500-bar window copy\n"
                "  series   = JSON parsed per bar, operands from SeriesOperandSource\n"
                "  compiled = CompiledConditions + SeriesOperandSource (what BacktestEngine runs)\n"
                "  floor    = CompiledConditions + operands that cost nothing\n\n");
    std::printf("%-26s %10s %10s %11s %9s %9s %10s %10s %7s   %s\n", "strategy", "old ms", "series ms",
                "compiled ms", "speedup", "floor ms", "diff bars", "run() ms", "trades", "verdicts#");

    for (const Strategy& s : strategies()) {
        std::vector<char> old_entry(bars), old_exit(bars), new_entry(bars), new_exit(bars);

        const auto o0 = clock::now();
        for (int i = kWarmupBars; i < bars; ++i) {
            const int start = std::max(0, i - kEvalWindow + 1);
            const QVector<OhlcvCandle> window = candles.mid(start, i - start + 1);
            old_entry[i] = ConditionEvaluator::evaluate_group(s.entry, s.entry_logic, window).triggered;
            old_exit[i] = ConditionEvaluator::evaluate_group(s.exit, s.exit_logic, window).triggered;
        }
        const auto old_time = clock::now() - o0;

        int series_sink = 0;
        const auto s0 = clock::now();
        {
            SeriesOperandSource operands(candles, kEvalWindow);
            for (int i = kWarmupBars; i < bars; ++i) {
                operands.set_bar(i);
                series_sink += ConditionEvaluator::evaluate_group(s.entry, s.entry_logic, operands).triggered;
                series_sink += ConditionEvaluator::evaluate_group(s.exit, s.exit_logic, operands).triggered;
            }
        }
        const auto series_time = clock::now() - s0;
        (void)series_sink;

        const auto n0 = clock::now();
        {
            SeriesOperandSource operands(candles, kEvalWindow);
            const fincept::algo::CompiledConditions entry(s.entry, s.entry_logic);
            const fincept::algo::CompiledConditions exit_rules(s.exit, s.exit_logic);
            for (int i = kWarmupBars; i < bars; ++i) {
                operands.set_bar(i);
                new_entry[i] = entry.evaluate(operands).triggered;
                new_exit[i] = exit_rules.evaluate(operands).triggered;
            }
        }
        const auto new_time = clock::now() - n0;

        int diff = 0;
        for (int i = kWarmupBars; i < bars; ++i)
            diff += (old_entry[i] != new_entry[i] || old_exit[i] != new_exit[i]) ? 1 : 0;

        // FNV-1a over the old path's per-bar verdicts: a fingerprint of the
        // evaluator's behaviour, to compare across changes to it.
        std::uint32_t verdicts = 2166136261u;
        for (int i = kWarmupBars; i < bars; ++i) {
            verdicts ^= static_cast<std::uint32_t>(old_entry[i] | (old_exit[i] << 1));
            verdicts *= 16777619u;
        }

        // Approximate: constant operands short-circuit AND/OR groups differently.
        ConstantOperands constant;
        int sink = 0;
        const auto f0 = clock::now();
        {
            const fincept::algo::CompiledConditions entry(s.entry, s.entry_logic);
            const fincept::algo::CompiledConditions exit_rules(s.exit, s.exit_logic);
            for (int i = kWarmupBars; i < bars; ++i) {
                sink += entry.evaluate(constant).triggered;
                sink += exit_rules.evaluate(constant).triggered;
            }
        }
        const auto floor_time = clock::now() - f0;
        (void)sink; // keeps the loop observable without printing it

        const auto r0 = clock::now();
        const QJsonObject result = BacktestEngine::run(candles, s.entry, s.entry_logic, s.exit, s.exit_logic, 0, 0, 0,
                                                       100'000.0, QStringLiteral("1d"));
        const auto run_time = clock::now() - r0;

        std::printf("%-26s %10.1f %10.1f %11.1f %8.0fx %9.1f %10d %10.1f %7d   %08x\n", s.name, ms(old_time),
                    ms(series_time), ms(new_time), ms(old_time) / ms(new_time), ms(floor_time), diff, ms(run_time),
                    result.value("total_trades").toInt(), verdicts);
    }
    return 0;
}
