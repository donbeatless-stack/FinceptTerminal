// Tests for src/python/PythonProcessPool.{h,cpp} together with the worker it
// drives, scripts/fincept_worker.py, against a real Python interpreter:
// FINCEPT_TEST_PYTHON if set, else python3 / python on PATH (skipped if none).
// The worker uses the standard library only.
//
// What a pooled call must keep from a fresh `python <script> <args>`: argv,
// __main__, sys.path[0], separate stdout/stderr, the exit code, a clean
// environment / working directory / sys.path for the next call, and the
// scripts tree's own modules re-imported (two directories with a core.py each).
// What the pool must guarantee: one callback per job, never from inside
// submit(); a job never re-run on a second worker; timeouts and crashes fail
// only the job that was running; Unavailable when no worker can start.

#include "python/PythonProcessPool.h"

#include <QCoreApplication>
#include <QDeadlineTimer>
#include <QDir>
#include <QElapsedTimer>
#include <QFile>
#include <QSet>
#include <QStandardPaths>
#include <QTemporaryDir>
#include <QTest>

#include <algorithm>
#include <functional>
#include <memory>

using fincept::python::PythonProcessPool;
using Outcome = PythonProcessPool::Outcome;
using Status = PythonProcessPool::Status;

namespace {

bool wait_for(const std::function<bool()>& cond, int ms = 20'000) {
    QDeadlineTimer deadline(ms);
    while (!cond()) {
        if (deadline.hasExpired())
            return false;
        QCoreApplication::processEvents(QEventLoop::AllEvents, 20);
        QTest::qSleep(1);
    }
    return true;
}

struct Answer {
    Outcome out;
    int calls = 0;
};

std::shared_ptr<Answer> submit(PythonProcessPool& pool, PythonProcessPool::Job job) {
    auto a = std::make_shared<Answer>();
    pool.submit(std::move(job), [a](Outcome o) {
        a->out = std::move(o);
        ++a->calls;
    });
    return a;
}

void write_file(const QString& path, const QByteArray& text) {
    QDir().mkpath(QFileInfo(path).absolutePath());
    QFile f(path);
    QVERIFY(f.open(QIODevice::WriteOnly | QIODevice::Truncate));
    f.write(text);
}

} // namespace

class TstPythonProcessPool : public QObject {
    Q_OBJECT

    QString python_;
    QTemporaryDir dir_;      // the "scripts tree": its modules are re-imported per call
    QTemporaryDir outside_;  // on PYTHONPATH, like site-packages: stays imported
    QStringList log_;

    QString tree(const QString& rel) const { return dir_.filePath(rel); }

    PythonProcessPool::Config config() {
        PythonProcessPool::Config c;
        c.worker_script = QStringLiteral(FINCEPT_WORKER_SCRIPT);
        c.tree = dir_.path();
        c.ready_timeout_ms = 15'000;
        c.log = [this](const QString& line) { log_ << line; };
        return c;
    }

    PythonProcessPool::Job job(const QString& rel_script, const QStringList& args = {}) const {
        PythonProcessPool::Job j;
        j.interpreter = python_;
        j.working_dir = dir_.path();
        j.env = QProcessEnvironment::systemEnvironment();
        j.env.insert(QStringLiteral("PYTHONPATH"), outside_.path());
        j.env.insert(QStringLiteral("PYTHONIOENCODING"), QStringLiteral("utf-8"));
        j.script = tree(rel_script);
        j.args = args;
        return j;
    }

    Outcome run(PythonProcessPool& pool, PythonProcessPool::Job j) {
        auto a = submit(pool, std::move(j));
        if (!wait_for([&] { return a->calls > 0; }))
            return {};
        return a->out;
    }

  private slots:
    void initTestCase();
    void init() { log_.clear(); }

    void runs_a_script_as_a_fresh_process_would();
    void exit_codes_and_exceptions();
    void keeps_outside_modules_and_reimports_the_trees_own();
    void each_call_starts_from_a_clean_process_state();
    void stray_writes_to_fd_1_do_not_corrupt_the_stream();
    void large_output();
    void module_form_with_relative_imports();
    void a_hung_script_times_out_and_only_its_job_fails();
    void a_crash_fails_only_that_job();
    void runs_jobs_in_parallel_up_to_max_workers();
    void a_job_stuck_behind_busy_workers_overflows();
    void recycles_after_max_requests();
    void recycles_when_resident_memory_passes_the_limit();
    void retires_idle_workers();
    void unavailable_when_no_worker_can_start();
    void unavailable_when_the_worker_never_reports_ready();
    void warm_imports_without_running_main();
    void shutdown_answers_every_job_once();
};

void TstPythonProcessPool::initTestCase() {
    python_ = qEnvironmentVariable("FINCEPT_TEST_PYTHON");
    // Not the Microsoft Store's python3.exe / python.exe stubs in WindowsApps:
    // they are on PATH by default and only print "Python was not found".
    QStringList path = qEnvironmentVariable("PATH").split(QDir::listSeparator(), Qt::SkipEmptyParts);
    path.erase(std::remove_if(path.begin(), path.end(),
                              [](const QString& d) { return d.contains("WindowsApps", Qt::CaseInsensitive); }),
               path.end());
    for (const char* name : {"python3", "python"}) {
        if (python_.isEmpty())
            python_ = QStandardPaths::findExecutable(QString::fromLatin1(name), path);
    }
    if (python_.isEmpty())
        QSKIP("no Python interpreter (set FINCEPT_TEST_PYTHON)");
    QVERIFY(QFileInfo::exists(QStringLiteral(FINCEPT_WORKER_SCRIPT)));
    QVERIFY(dir_.isValid() && outside_.isValid());

    write_file(tree("echo.py"), R"(import os, sys
print("argv", sys.argv[1:])
print("name", __name__)
print("path0", os.path.normcase(sys.path[0]) == os.path.normcase(os.path.dirname(os.path.abspath(__file__))))
print("cwd", os.path.normcase(os.getcwd()))
print("env", os.environ.get("FINCEPT_POOL_TEST", "-"))
print("stdin", sys.stdin.read())
print("to stderr", file=sys.stderr)
)");
    write_file(tree("exits.py"), R"(import sys
mode = sys.argv[1]
print("before")
if mode == "code": sys.exit(3)
if mode == "none": sys.exit()
if mode == "text": sys.exit("bad input")
if mode == "raise": raise ValueError("boom")
)");
    write_file(outside_.filePath("heavy.py"), "IMPORTS = 0\nCALLS = 0\nHELPER_LOADS = 0\n");
    write_file(tree("uses_heavy.py"), R"(import heavy, helper
heavy.CALLS += 1
print(heavy.CALLS, heavy.HELPER_LOADS)
)");
    write_file(tree("helper.py"), "import heavy\nheavy.HELPER_LOADS += 1\n");
    write_file(tree("a/core.py"), "NAME = 'a'\n");
    write_file(tree("a/run.py"), "import core\nprint(core.NAME)\n");
    write_file(tree("b/core.py"), "NAME = 'b'\n");
    write_file(tree("b/run.py"), "import core\nprint(core.NAME)\n");
    write_file(tree("mutate.py"), R"(import os, sys, warnings
os.environ["FINCEPT_LEAK"] = "1"
os.makedirs("sub", exist_ok=True)
os.chdir("sub")
sys.path.insert(0, "zzz-leak")
warnings.simplefilter("error")
print("mutated")
)");
    write_file(tree("inspect_state.py"), R"(import os, sys, warnings
print("leak", os.environ.get("FINCEPT_LEAK", "-"))
print("cwd", os.path.basename(os.getcwd()))
print("path", "zzz-leak" in sys.path)
warnings.warn("just a warning")
print("warned")
)");
    write_file(tree("stray.py"), R"(import os, sys
os.write(1, b"\x00\x00\x00\x05garbage written to fd 1\n")
print("ok")
)");
    write_file(tree("big.py"), "import sys\nsys.stdout.write('x' * (8 * 1024 * 1024))\nprint()\nprint('end')\n");
    write_file(tree("pkg/__init__.py"), "");
    write_file(tree("pkg/util.py"), "X = 42\n");
    write_file(tree("pkg/mod.py"), "from .util import X\nprint('module', __name__, X)\n");
    write_file(tree("sleep.py"), "import os, sys, time\ntime.sleep(float(sys.argv[1]))\nprint(os.getpid())\n");
    write_file(tree("die.py"), "import os\nprint('partial')\nos._exit(5)\n");
    write_file(tree("pid.py"), "import os\nprint(os.getpid())\n");
    write_file(tree("not_a_worker.py"), "import time\ntime.sleep(60)\n");
}

void TstPythonProcessPool::runs_a_script_as_a_fresh_process_would() {
    PythonProcessPool pool(config());
    const QString json = QStringLiteral("{\"a\": [1, \"") + QChar(0xE9) + QStringLiteral("\"]}");
    const QString expected_argv = QStringLiteral("argv ['check_status', '") + json + QStringLiteral("']");
    auto j = job("echo.py", {"check_status", json});
    j.env.insert("FINCEPT_POOL_TEST", "from-the-job");
    j.stdin_data = "payload on stdin";
    const Outcome o = run(pool, j);
    QCOMPARE(o.status, Status::Completed);
    QVERIFY2(o.exit_code == 0, qPrintable(o.stderr_text));
    QVERIFY2(o.stdout_text.contains(expected_argv), qPrintable(o.stdout_text));
    QVERIFY(o.stdout_text.contains("name __main__"));
    QVERIFY(o.stdout_text.contains("path0 True"));
    QVERIFY(o.stdout_text.contains("env from-the-job"));
    QVERIFY(o.stdout_text.contains("stdin payload on stdin"));
    QVERIFY(!o.stdout_text.contains("to stderr"));
    QCOMPARE(o.stderr_text.trimmed(), QString("to stderr"));
    QVERIFY(o.worker_pid > 0);
    QVERIFY(!o.warm);
}

void TstPythonProcessPool::exit_codes_and_exceptions() {
    PythonProcessPool pool(config());
    Outcome o = run(pool, job("exits.py", {"code"}));
    QCOMPARE(o.status, Status::Completed);
    QCOMPARE(o.exit_code, 3);
    QCOMPARE(o.stdout_text, QString("before\n")); // output before the exit is kept
    QCOMPARE(run(pool, job("exits.py", {"none"})).exit_code, 0);
    o = run(pool, job("exits.py", {"text"}));
    QCOMPARE(o.exit_code, 1);
    QCOMPARE(o.stderr_text.trimmed(), QString("bad input"));
    o = run(pool, job("exits.py", {"raise"}));
    QCOMPARE(o.exit_code, 1);
    QVERIFY(o.stderr_text.contains("ValueError: boom"));
    QVERIFY(o.stderr_text.contains("Traceback"));
    // None of that cost the worker.
    QCOMPARE(pool.stats().spawned, quint64{1});
}

void TstPythonProcessPool::keeps_outside_modules_and_reimports_the_trees_own() {
    PythonProcessPool pool(config());
    const Outcome first = run(pool, job("uses_heavy.py"));
    const Outcome second = run(pool, job("uses_heavy.py"));
    const Outcome third = run(pool, job("uses_heavy.py"));
    // heavy (outside the tree) stayed imported: its counters carry over. helper
    // (inside) was imported afresh each time: it bumped HELPER_LOADS each call.
    QCOMPARE(first.stdout_text.trimmed(), QString("1 1"));
    QCOMPARE(second.stdout_text.trimmed(), QString("2 2"));
    QCOMPARE(third.stdout_text.trimmed(), QString("3 3"));
    QCOMPARE(second.worker_pid, first.worker_pid);
    QVERIFY(!first.warm);
    QVERIFY(second.warm);

    // Two directories, each with its own core.py: a cached `core` would hand b
    // a's module.
    QCOMPARE(run(pool, job("a/run.py")).stdout_text.trimmed(), QString("a"));
    QCOMPARE(run(pool, job("b/run.py")).stdout_text.trimmed(), QString("b"));
    QCOMPARE(run(pool, job("a/run.py")).stdout_text.trimmed(), QString("a"));
    QCOMPARE(pool.stats().spawned, quint64{1});
}

void TstPythonProcessPool::each_call_starts_from_a_clean_process_state() {
    PythonProcessPool pool(config());
    QCOMPARE(run(pool, job("mutate.py")).stdout_text.trimmed(), QString("mutated"));
    const Outcome o = run(pool, job("inspect_state.py"));
    QCOMPARE(o.exit_code, 0); // simplefilter("error") would have turned the warning into an exception
    QVERIFY2(o.stdout_text.contains("leak -"), qPrintable(o.stdout_text));
    QVERIFY(o.stdout_text.contains("cwd " + QFileInfo(dir_.path()).fileName()));
    QVERIFY(o.stdout_text.contains("path False"));
    QVERIFY(o.stdout_text.contains("warned"));
    QCOMPARE(pool.stats().spawned, quint64{1});
}

void TstPythonProcessPool::stray_writes_to_fd_1_do_not_corrupt_the_stream() {
    // Bytes that look like a frame header, written straight to fd 1 (what a C
    // extension or a child process would do), must not reach the host's frame
    // parser.
    PythonProcessPool pool(config());
    const Outcome o = run(pool, job("stray.py"));
    QCOMPARE(o.status, Status::Completed);
    QCOMPARE(o.stdout_text, QString("ok\n"));
    const Outcome next = run(pool, job("pid.py"));
    QCOMPARE(next.status, Status::Completed);
    QCOMPARE(next.worker_pid, o.worker_pid);
}

void TstPythonProcessPool::large_output() {
    PythonProcessPool pool(config());
    const Outcome o = run(pool, job("big.py"));
    QCOMPARE(o.status, Status::Completed);
    QCOMPARE(o.stdout_text.size(), 8 * 1024 * 1024 + 1 + 4);
    QVERIFY(o.stdout_text.endsWith("\nend\n"));
}

void TstPythonProcessPool::module_form_with_relative_imports() {
    PythonProcessPool pool(config());
    auto j = job("pid.py");
    j.script.clear();
    j.module = "pkg.mod";
    const Outcome o = run(pool, j);
    QCOMPARE(o.status, Status::Completed);
    QCOMPARE(o.stdout_text.trimmed(), QString("module __main__ 42"));
}

void TstPythonProcessPool::a_hung_script_times_out_and_only_its_job_fails() {
    auto c = config();
    c.max_workers = 1;
    PythonProcessPool pool(c);
    auto hung = job("sleep.py", {"30"});
    hung.timeout_ms = 1500;
    auto a = submit(pool, hung);
    // Queued behind it on the only worker for longer than its own budget: the
    // budget covers running, not waiting (as for a spawned process).
    auto quick = job("pid.py");
    quick.timeout_ms = 1000;
    auto b = submit(pool, quick);
    QElapsedTimer t;
    t.start();
    QVERIFY(wait_for([&] { return a->calls && b->calls; }));
    QCOMPARE(a->calls, 1);
    QCOMPARE(a->out.status, Status::Failed);
    QVERIFY(a->out.timed_out);
    QVERIFY2(a->out.error.contains("timed out after 1 s"), qPrintable(a->out.error));
    // The queued job ran, in a new worker.
    QCOMPARE(b->out.status, Status::Completed);
    QVERIFY(b->out.worker_pid != a->out.worker_pid);
    QVERIFY(t.elapsed() < 15'000);
    QCOMPARE(pool.stats().timed_out, quint64{1});
}

void TstPythonProcessPool::a_crash_fails_only_that_job() {
    PythonProcessPool pool(config());
    const Outcome dead = run(pool, job("die.py"));
    QCOMPARE(dead.status, Status::Failed);
    QVERIFY(!dead.timed_out);
    QVERIFY2(dead.error.contains("exited with code 5"), qPrintable(dead.error));
    const Outcome next = run(pool, job("pid.py"));
    QCOMPARE(next.status, Status::Completed);
    QVERIFY(next.worker_pid != dead.worker_pid);
    QCOMPARE(pool.stats().failed, quint64{1});
}

void TstPythonProcessPool::runs_jobs_in_parallel_up_to_max_workers() {
    auto c = config();
    c.max_workers = 2;
    PythonProcessPool pool(c);
    // Start both workers first so the timing below measures the jobs alone.
    auto w1 = submit(pool, job("sleep.py", {"0.3"}));
    auto w2 = submit(pool, job("sleep.py", {"0.3"}));
    QVERIFY(wait_for([&] { return w1->calls && w2->calls; }));

    QElapsedTimer t;
    t.start();
    std::vector<std::shared_ptr<Answer>> as;
    for (int i = 0; i < 4; ++i)
        as.push_back(submit(pool, job("sleep.py", {"0.8"})));
    QCOMPARE(pool.stats().busy, 2);
    QCOMPARE(pool.stats().queued, 2);
    QVERIFY(wait_for([&] { return std::all_of(as.begin(), as.end(), [](auto& a) { return a->calls == 1; }); }));
    const qint64 ms = t.elapsed();
    QSet<qint64> pids;
    for (auto& a : as) {
        QCOMPARE(a->out.status, Status::Completed);
        pids.insert(a->out.worker_pid);
    }
    QCOMPARE(pids.size(), 2);
    QCOMPARE(pool.stats().spawned, quint64{2});
    QVERIFY2(ms >= 1500 && ms < 3000, qPrintable(QString::number(ms))); // two rounds of 0.8 s, not one or four
}

void TstPythonProcessPool::a_job_stuck_behind_busy_workers_overflows() {
    // The only worker is busy with a long call: a quick one queued behind it is
    // handed back (Unavailable, to run as its own process) after max_queue_wait_ms
    // rather than after the long call.
    auto c = config();
    c.max_workers = 1;
    c.max_queue_wait_ms = 500;
    PythonProcessPool pool(c);
    auto busy = submit(pool, job("sleep.py", {"3"}));
    QVERIFY(wait_for([&] { return pool.stats().busy == 1; }));
    QElapsedTimer t;
    t.start();
    auto waiting = submit(pool, job("pid.py"));
    QVERIFY(wait_for([&] { return waiting->calls > 0; }));
    const qint64 ms = t.elapsed();
    QCOMPARE(waiting->out.status, Status::Unavailable);
    QVERIFY2(waiting->out.error.contains("busy"), qPrintable(waiting->out.error));
    QVERIFY2(ms >= 400 && ms < 2'000, qPrintable(QString::number(ms)));
    QVERIFY(wait_for([&] { return busy->calls > 0; }));
    QCOMPARE(busy->out.status, Status::Completed); // the long call is not disturbed
    QCOMPARE(pool.stats().overflowed, quint64{1});
    QCOMPARE(waiting->calls, 1);
}

void TstPythonProcessPool::recycles_after_max_requests() {
    auto c = config();
    c.max_requests_per_worker = 2;
    PythonProcessPool pool(c);
    const qint64 p1 = run(pool, job("pid.py")).worker_pid;
    const qint64 p2 = run(pool, job("pid.py")).worker_pid;
    const qint64 p3 = run(pool, job("pid.py")).worker_pid;
    QCOMPARE(p2, p1);
    QVERIFY(p3 != p1);
    QCOMPARE(pool.stats().retired, quint64{1});
    QVERIFY(wait_for([&] { return pool.stats().workers == 1; }));
}

void TstPythonProcessPool::recycles_when_resident_memory_passes_the_limit() {
    // Any interpreter is over 1 MB resident, so every worker goes after one call.
    auto c = config();
    c.max_worker_mb = 1;
    PythonProcessPool pool(c);
    const qint64 p1 = run(pool, job("pid.py")).worker_pid;
    const qint64 p2 = run(pool, job("pid.py")).worker_pid;
    QVERIFY(p1 > 0 && p2 > 0 && p1 != p2);
    QCOMPARE(pool.stats().retired, quint64{2});
    QVERIFY(log_.join(QLatin1Char('\n')).contains("MB resident"));
}

void TstPythonProcessPool::retires_idle_workers() {
    auto c = config();
    c.idle_timeout_ms = 400;
    PythonProcessPool pool(c);
    QCOMPARE(run(pool, job("pid.py")).status, Status::Completed);
    QCOMPARE(pool.stats().workers, 1);
    QVERIFY(wait_for([&] { return pool.stats().workers == 0; }, 5'000));
    QCOMPARE(pool.stats().retired, quint64{1});
    // And the next job simply starts a new one.
    QCOMPARE(run(pool, job("pid.py")).status, Status::Completed);
}

void TstPythonProcessPool::unavailable_when_no_worker_can_start() {
    PythonProcessPool pool(config());
    auto j = job("pid.py");
    j.interpreter = dir_.filePath("no-such-python.exe");
    auto a = submit(pool, j);
    QCOMPARE(a->calls, 0); // never from inside submit()
    QVERIFY(wait_for([&] { return a->calls > 0; }));
    QCOMPARE(a->out.status, Status::Unavailable);
    QVERIFY2(a->out.error.contains("could not start"), qPrintable(a->out.error));
    // In the cool-down: answered at once, no second attempt to start one.
    auto b = submit(pool, j);
    QCOMPARE(b->calls, 0);
    QVERIFY(wait_for([&] { return b->calls > 0; }, 1'000));
    QCOMPARE(b->out.status, Status::Unavailable);
    QCOMPARE(pool.stats().spawned, quint64{1});
    QCOMPARE(a->calls, 1);
}

void TstPythonProcessPool::unavailable_when_the_worker_never_reports_ready() {
    auto c = config();
    c.worker_script = tree("not_a_worker.py");
    c.ready_timeout_ms = 800;
    PythonProcessPool pool(c);
    const Outcome o = run(pool, job("pid.py"));
    QCOMPARE(o.status, Status::Unavailable);
    QVERIFY2(o.error.contains("did not report ready"), qPrintable(o.error));
}

void TstPythonProcessPool::warm_imports_without_running_main() {
    write_file(tree("warmable.py"), R"(import heavy
heavy.IMPORTS += 1
if __name__ == "__main__":
    with open("ran.txt", "a") as f:
        f.write("main\n")
    print("imports", heavy.IMPORTS)
)");
    QFile::remove(tree("ran.txt"));
    PythonProcessPool pool(config());
    pool.warm(job("warmable.py"));
    pool.warm(job("warmable.py")); // already queued: ignored
    QVERIFY(wait_for([&] { return log_.join('\n').contains("warmed"); }));
    QCOMPARE(log_.filter("warmed").size(), 1);
    QVERIFY(!QFile::exists(tree("ran.txt"))); // main did not run
    pool.warm(job("warmable.py"));            // the worker has it: ignored
    const Outcome o = run(pool, job("warmable.py"));
    QCOMPARE(o.status, Status::Completed);
    QVERIFY(o.warm);
    QCOMPARE(o.stdout_text.trimmed(), QString("imports 2")); // the module body ran once more, main once
    QFile ran(tree("ran.txt"));
    QVERIFY(ran.open(QIODevice::ReadOnly));
    QCOMPARE(QString::fromUtf8(ran.readAll()).trimmed(), QString("main")); // once
    QCOMPARE(pool.stats().warm_hits, quint64{1});
    QCOMPARE(pool.stats().spawned, quint64{1});
}

void TstPythonProcessPool::shutdown_answers_every_job_once() {
    auto c = config();
    c.max_workers = 1;
    PythonProcessPool pool(c);
    auto running = submit(pool, job("sleep.py", {"30"}));
    auto queued = submit(pool, job("pid.py"));
    QVERIFY(wait_for([&] { return pool.stats().busy == 1; }));
    pool.shutdown();
    QVERIFY(wait_for([&] { return running->calls && queued->calls; }));
    QCOMPARE(running->out.status, Status::Failed);
    QCOMPARE(queued->out.status, Status::Unavailable);
    auto later = submit(pool, job("pid.py"));
    QVERIFY(wait_for([&] { return later->calls > 0; }));
    QCOMPARE(later->out.status, Status::Unavailable);
    QTest::qWait(200);
    QCOMPARE(running->calls, 1);
    QCOMPARE(queued->calls, 1);
    QCOMPARE(pool.stats().workers, 0);
}

QTEST_GUILESS_MAIN(TstPythonProcessPool)
#include "tst_python_process_pool.moc"
