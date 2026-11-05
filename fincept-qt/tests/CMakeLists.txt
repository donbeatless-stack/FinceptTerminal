# ── Fincept Terminal unit tests (Qt Test) ────────────────────────────────────
#
# Reached ONLY from the top-level CMakeLists.txt under `if(FINCEPT_BUILD_TESTS)`,
# which is OFF by default. Nothing in this file affects a normal build.
#
#   cmake -B build -DFINCEPT_BUILD_TESTS=ON ...
#   cmake --build build
#   ctest --test-dir build --output-on-failure          # add -C Release on MSVC
#
# ─────────────────────────────────────────────────────────────────────────────
# HARD RULE — read this before adding a target.
#
#   A test target lists the SPECIFIC sources it needs, explicitly, by path.
#
# Do NOT "solve" a missing symbol by refactoring the app into an OBJECT library
# and linking it here. The release/CI presets build with CMAKE_UNITY_BUILD=ON
# (batch size 20) while `win-dev` builds unity OFF, so any change to the main
# source list re-batches every file after it and can produce file-scope symbol
# redefinitions that appear ONLY on CI and never reproduce locally. See the
# "unity build" notes in ../CLAUDE.md.
#
# If a unit cannot be tested without dragging in half the application, that is a
# signal about the unit, not about this file: extract the pure logic into a small
# header (or a leaf .cpp) and test that. Do not grow the link line.
#
# Current cost of the whole suite: 8 test executables + 3 benchmarks, 43 translation units.
# ─────────────────────────────────────────────────────────────────────────────

# Qt Test ships with every Qt installation, so this adds no new dependency.
# _FINCEPT_QT_VER_ARGS is inherited from the parent scope so the version pin
# (6.8.3 EXACT by default) stays identical to the one the app is built against.
find_package(Qt6 ${_FINCEPT_QT_VER_ARGS} QUIET COMPONENTS Test)
if(NOT Qt6Test_FOUND)
    message(FATAL_ERROR
        "FINCEPT_BUILD_TESTS=ON but the Qt6 Test module was not found.\n"
        "Qt Test ships with the standard Qt installer — re-run the Qt maintenance "
        "tool and make sure 'Qt Test' is selected for your kit, or configure with "
        "-DFINCEPT_BUILD_TESTS=OFF.")
endif()

# Windows has no rpath: a freshly built tst_*.exe cannot find Qt6Core.dll unless
# the Qt bin dir is on PATH. Resolve it once and inject it per-test below, so
# `ctest` works straight out of a fresh configure without anyone having to run
# windeployqt against the test binaries.
#
# Best-effort by design: if the Qt layout is unusual (distro split packages, no
# qmake shipped) we simply skip the injection rather than fail the configure —
# on those layouts Qt is on the loader path already.
set(_fincept_qt_bindir "")
set(_fincept_qt_libdir "")
if(TARGET Qt6::qmake)
    get_target_property(_fincept_qt_qmake Qt6::qmake IMPORTED_LOCATION)
    if(_fincept_qt_qmake AND EXISTS "${_fincept_qt_qmake}")
        get_filename_component(_fincept_qt_bindir "${_fincept_qt_qmake}" DIRECTORY)
        get_filename_component(_fincept_qt_prefix "${_fincept_qt_bindir}" DIRECTORY)
        set(_fincept_qt_libdir "${_fincept_qt_prefix}/lib")
    endif()
endif()
if(NOT _fincept_qt_bindir AND Qt6_DIR)
    # Qt6_DIR is <prefix>/lib/cmake/Qt6 for a standard installer layout.
    get_filename_component(_fincept_qt_prefix "${Qt6_DIR}/../../.." ABSOLUTE)
    if(EXISTS "${_fincept_qt_prefix}/bin")
        set(_fincept_qt_bindir "${_fincept_qt_prefix}/bin")
        set(_fincept_qt_libdir "${_fincept_qt_prefix}/lib")
    endif()
endif()

# fincept_add_test(<name> <source> [<extra sources>...])
#
# <name> must be tst_<unit>. Extra sources are the app .cpp files this unit
# genuinely needs — keep that list as short as it can possibly be.
function(fincept_add_test _name)
    add_executable(${_name} ${ARGN})

    # Qt6::Test pulls in Qt6::Core. Deliberately no Widgets/Network/Sql here:
    # a unit test that needs those is testing the wrong seam.
    target_link_libraries(${_name} PRIVATE Qt6::Test Qt6::Core)

    # Sources use project-root-relative includes ("trading/OrderValidator.h"),
    # matching target_include_directories(FinceptTerminal PRIVATE src).
    target_include_directories(${_name} PRIVATE "${PROJECT_SOURCE_DIR}/src")

    set_target_properties(${_name} PROPERTIES
        AUTOMOC ON            # Q_OBJECT test classes live in the .cpp (+ #include "<name>.moc")
        UNITY_BUILD OFF       # never batch test TUs, whatever the parent preset says
        FOLDER "tests")

    # NOTE: no install() rule, by design. Test executables must never ship in a
    # release artifact. They also land in ${CMAKE_CURRENT_BINARY_DIR} (build/tests/),
    # not next to FinceptTerminal.exe, so the installer's
    # install(DIRECTORY "$<TARGET_FILE_DIR:FinceptTerminal>/" ... FILES_MATCHING "*.dll")
    # rule cannot pick them up either.

    add_test(NAME ${_name} COMMAND ${_name})

    if(WIN32 AND _fincept_qt_bindir)
        set_tests_properties(${_name} PROPERTIES
            ENVIRONMENT_MODIFICATION "PATH=path_list_prepend:${_fincept_qt_bindir}")
    elseif(UNIX AND NOT APPLE AND _fincept_qt_libdir)
        set_tests_properties(${_name} PROPERTIES
            ENVIRONMENT_MODIFICATION "LD_LIBRARY_PATH=path_list_prepend:${_fincept_qt_libdir}")
    endif()
endfunction()

# ── Suites ───────────────────────────────────────────────────────────────────

# src/trading/brokers/BrokerModifyFields.h — header-only, zero app sources.
fincept_add_test(tst_broker_modify_fields
    tst_broker_modify_fields.cpp)

# src/core/result/Result.h — header-only, zero app sources.
fincept_add_test(tst_result
    tst_result.cpp)

# src/trading/OrderValidator.{h,cpp} — the .cpp includes only its own header,
# which includes only trading/TradingTypes.h (Qt Core types + plain structs, no
# .cpp of its own). One extra TU, no service/network/broker-registry linkage.
# The risk gate's settings I/O and account snapshot live in
# OrderValidatorRiskGate.cpp precisely so this target never needs Sql/storage;
# do not add that file here.
fincept_add_test(tst_order_validator
    tst_order_validator.cpp
    "${PROJECT_SOURCE_DIR}/src/trading/OrderValidator.cpp")

# src/algo_engine/IndicatorEngine.{h,cpp}: the .cpp includes only its own
# header, which includes only algo_engine/AlgoEngineTypes.h (Qt Core types +
# plain structs, no .cpp of its own). One extra TU, same shape as the validator
# suite above.
fincept_add_test(tst_indicator_engine
    tst_indicator_engine.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")

# src/algo_engine/StreamingIndicators.{h,cpp}: plain C++, no Qt. IndicatorEngine
# is linked as the oracle the streaming classes are checked against bar by bar.
fincept_add_test(tst_streaming_indicators
    tst_streaming_indicators.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingIndicators.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")

# Timing comparison of the two (machine-dependent, so NOT registered with ctest):
#   build/<preset>/tests/bench_streaming_indicators [bars]
add_executable(bench_streaming_indicators
    bench_streaming_indicators.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingIndicators.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")
target_link_libraries(bench_streaming_indicators PRIVATE Qt6::Core)
target_include_directories(bench_streaming_indicators PRIVATE "${PROJECT_SOURCE_DIR}/src")
set_target_properties(bench_streaming_indicators PROPERTIES UNITY_BUILD OFF FOLDER "tests")

# src/algo_engine/{OperandSource,SeriesOperandSource}: the operand layer
# BacktestEngine evaluates conditions through. Tested below ConditionEvaluator on
# purpose: the evaluator's header pulls services/algo_trading/AlgoTradingTypes.h
# (QColor, i.e. Qt Gui), and its verdicts are a pure function of these operands.
fincept_add_test(tst_series_operand_source
    tst_series_operand_source.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/OperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/SeriesOperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingOperand.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingIndicators.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")

# src/algo_engine/LiveOperandSource.{h,cpp}: RealtimeScanRunner's per-symbol
# operand state (warm-up, live closes, tick bars), against WindowOperandSource.
fincept_add_test(tst_live_operand_source
    tst_live_operand_source.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/LiveOperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingOperand.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingIndicators.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/OperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")

# src/trading/brokers/alpaca/AlpacaBarsPaging.h: AlpacaBroker::get_history's
# next_page_token loop and time parameters, with scripted responses (no network).
fincept_add_test(tst_alpaca_bars_paging
    tst_alpaca_bars_paging.cpp)

# src/algo_engine/GatewayHistory.{h,cpp}: the gateway HISTORY reply parser and the
# rule for which symbols it may stand in for the broker (fresh sync, whole periods,
# enough history).
fincept_add_test(tst_gateway_history
    tst_gateway_history.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/GatewayHistory.cpp")

# src/python/PythonProcessPool.{h,cpp} with scripts/fincept_worker.py, against a real
# interpreter (FINCEPT_TEST_PYTHON, else python3/python on PATH; skipped if none):
# fresh-process semantics per call, timeouts, crashes, recycling, Unavailable.
fincept_add_test(tst_python_process_pool
    tst_python_process_pool.cpp
    "${PROJECT_SOURCE_DIR}/src/python/PythonProcessPool.cpp")
target_compile_definitions(tst_python_process_pool PRIVATE
    FINCEPT_WORKER_SCRIPT="${PROJECT_SOURCE_DIR}/scripts/fincept_worker.py")

# AI Quant Lab calls as fresh processes vs through the pool, with the app's venvs and
# scripts (machine-dependent, so NOT registered with ctest):
#   build/<preset>/tests/bench_python_pool [scripts_dir] [venv_root]
add_executable(bench_python_pool
    bench_python_pool.cpp
    "${PROJECT_SOURCE_DIR}/src/python/PythonProcessPool.cpp")
target_link_libraries(bench_python_pool PRIVATE Qt6::Core)
target_include_directories(bench_python_pool PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_compile_definitions(bench_python_pool PRIVATE FINCEPT_SCRIPTS_DIR="${PROJECT_SOURCE_DIR}/scripts")
set_target_properties(bench_python_pool PROPERTIES UNITY_BUILD OFF AUTOMOC ON FOLDER "tests")

# src/algo_engine/ConditionEvaluator.{h,cpp}: operator semantics, offsets, error
# propagation, AND/OR short-circuiting and nested groups, against a scripted
# OperandSource. Links Qt6::Gui only for headers:
# ConditionEvaluator.h -> services/algo_trading/AlgoTradingTypes.h -> <QColor>.
fincept_add_test(tst_condition_evaluator
    tst_condition_evaluator.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/ConditionEvaluator.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/OperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")
target_link_libraries(tst_condition_evaluator PRIVATE Qt6::Gui)

# IndicatorEngine::warmup_bars against compute() for every indicator in the
# strategy builder's catalog (defaults, range ends, random points: exact minimum),
# and ConditionEvaluator::required_bars against the window operand source. Qt6::Gui
# for the same header reason as above.
fincept_add_test(tst_indicator_warmup
    tst_indicator_warmup.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/ConditionEvaluator.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/OperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")
target_link_libraries(tst_indicator_warmup PRIVATE Qt6::Gui)

# Old-vs-new condition evaluation over whole strategies, plus BacktestEngine::run
# end to end. A benchmark, not a test, so it may link the evaluator (Qt Gui
# headers) and the logger; NOT registered with ctest.
add_executable(bench_backtest_eval
    bench_backtest_eval.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/BacktestEngine.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/ConditionEvaluator.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/OperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/SeriesOperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingOperand.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingIndicators.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp"
    "${PROJECT_SOURCE_DIR}/src/core/logging/Logger.cpp")
target_link_libraries(bench_backtest_eval PRIVATE Qt6::Core Qt6::Gui)
target_include_directories(bench_backtest_eval PRIVATE "${PROJECT_SOURCE_DIR}/src")
set_target_properties(bench_backtest_eval PROPERTIES UNITY_BUILD OFF FOLDER "tests")

# RealtimeScanRunner's per-sweep cost across a symbol universe and its drift from
# full-history indicator values, old path vs LiveOperandSource. NOT in ctest.
add_executable(bench_realtime_scan
    bench_realtime_scan.cpp
    "${PROJECT_SOURCE_DIR}/src/algo_engine/ConditionEvaluator.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/LiveOperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingOperand.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/StreamingIndicators.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/OperandSource.cpp"
    "${PROJECT_SOURCE_DIR}/src/algo_engine/IndicatorEngine.cpp")
target_link_libraries(bench_realtime_scan PRIVATE Qt6::Core Qt6::Gui)
target_include_directories(bench_realtime_scan PRIVATE "${PROJECT_SOURCE_DIR}/src")
set_target_properties(bench_realtime_scan PROPERTIES UNITY_BUILD OFF FOLDER "tests")

message(STATUS "Fincept tests: tst_broker_modify_fields, tst_result, tst_order_validator, tst_indicator_engine, "
               "tst_streaming_indicators, tst_series_operand_source, tst_condition_evaluator, "
               "tst_live_operand_source, tst_indicator_warmup, tst_alpaca_bars_paging, tst_gateway_history, "
               "tst_python_process_pool "
               "(+ bench_streaming_indicators, bench_backtest_eval, bench_realtime_scan, bench_python_pool)")
