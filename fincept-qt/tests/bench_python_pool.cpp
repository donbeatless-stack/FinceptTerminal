// AI Quant Lab calls, run the old way (a fresh `python <script> <command> <json>`
// per call, as PythonRunner spawns it) and through PythonProcessPool, with the
// app's own venvs and scripts. Prints, per call: the fresh-process time (best of
// 3), the first pooled call in a new worker (worker start + the script's
// imports), the median of 5 repeat calls in that worker, and whether the pooled
// result is byte-identical to the fresh process's.
//
//   bench_python_pool [scripts_dir] [venv_root]
//     scripts_dir  default: this checkout's fincept-qt/scripts
//     venv_root    default: %LOCALAPPDATA%/com.fincept.terminal (venv-numpy1/2)
//
// Machine-dependent, so not registered with ctest.

#include "python/PythonProcessPool.h"

#include <QCoreApplication>
#include <QDir>
#include <QElapsedTimer>
#include <QEventLoop>
#include <QFileInfo>
#include <QProcess>
#include <QStandardPaths>
#include <QTextStream>

#include <algorithm>
#include <cmath>
#include <vector>

using fincept::python::PythonProcessPool;

namespace {

struct Case {
    const char* venv;
    const char* script;
    QStringList args;
};

QString last_line(const QString& s) {
    const QStringList lines = s.trimmed().split('\n');
    return lines.isEmpty() ? QString() : lines.last().trimmed();
}

QProcessEnvironment env_for(const QString& scripts) {
    QProcessEnvironment env = QProcessEnvironment::systemEnvironment();
    env.insert("PYTHONIOENCODING", "utf-8");
    env.insert("PYTHONDONTWRITEBYTECODE", "1");
    env.insert("PYTHONUNBUFFERED", "1");
    env.insert("PYTHONPATH", scripts);
    return env;
}

struct Fresh {
    double ms = 0;
    QString out;
    int exit_code = -1;
};

Fresh run_fresh(const QString& py, const QString& scripts, const Case& c) {
    QProcess p;
    p.setProgram(py);
    p.setArguments(QStringList{scripts + "/" + c.script} + c.args);
    p.setWorkingDirectory(scripts);
    p.setProcessEnvironment(env_for(scripts));
#ifdef Q_OS_WIN
    p.setCreateProcessArgumentsModifier(
        [](QProcess::CreateProcessArguments* a) { a->flags |= 0x08000000; /* CREATE_NO_WINDOW */ });
#endif
    QElapsedTimer t;
    t.start();
    p.start();
    p.waitForFinished(10 * 60 * 1000);
    return {t.nsecsElapsed() / 1e6, QString::fromUtf8(p.readAllStandardOutput()), p.exitCode()};
}

struct Pooled {
    double ms = 0;
    PythonProcessPool::Outcome out;
};

Pooled run_pooled(PythonProcessPool& pool, const QString& py, const QString& scripts, const Case& c) {
    PythonProcessPool::Job job;
    job.interpreter = py;
    job.working_dir = scripts;
    job.env = env_for(scripts);
    job.script = scripts + "/" + c.script;
    job.args = c.args;
    Pooled r;
    QEventLoop loop;
    QElapsedTimer t;
    t.start();
    pool.submit(job, [&](PythonProcessPool::Outcome o) {
        r.ms = t.nsecsElapsed() / 1e6;
        r.out = std::move(o);
        loop.quit();
    });
    loop.exec();
    return r;
}

} // namespace

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    const QString scripts = argc > 1 ? QString::fromLocal8Bit(argv[1]) : QStringLiteral(FINCEPT_SCRIPTS_DIR);
    const QString venvs =
        argc > 2 ? QString::fromLocal8Bit(argv[2])
                 : QDir(QStandardPaths::writableLocation(QStandardPaths::GenericDataLocation))
                       .filePath("com.fincept.terminal");
#ifdef Q_OS_WIN
    const QString py_rel = "Scripts/python.exe";
#else
    const QString py_rel = "bin/python3";
#endif

    const QString cov = R"({"assets": ["A", "B", "C", "D"], "cov_matrix": [[0.040, 0.006, 0.012, 0.004],
        [0.006, 0.090, 0.018, 0.010], [0.012, 0.018, 0.0625, 0.008], [0.004, 0.010, 0.008, 0.0225]]})";
    QString series;
    for (int i = 0; i < 250; ++i)
        series += (i ? "," : "") + QString::number(0.0004 + 0.012 * std::sin(i * 0.7) * std::cos(i * 0.13), 'f', 6);
    const QString values = "{\"values\": [" + series + "]}";
    const QString returns = "{\"returns\": [" + series + "], \"confidence_level\": 0.95}";

    const std::vector<Case> cases = {
        {"venv-numpy2", "ai_quant_lab/qlib_portfolio_opt.py", {"minimum_variance", cov}},
        {"venv-numpy2", "ai_quant_lab/qlib_portfolio_opt.py", {"hierarchical_risk_parity", cov}},
        {"venv-numpy2", "ai_quant_lab/qlib_evaluation.py", {"calculate_risk_metrics", returns}},
        {"venv-numpy2", "Analytics/statsmodels_wrapper/statsmodels_service.py", {"descriptive", values}},
        {"venv-numpy2", "ai_quant_lab/qlib_service.py", {"check_status", "{}"}},
        {"venv-numpy2", "ai_quant_lab/qlib_meta_learning.py", {"check_status", "{}"}},
        {"venv-numpy1", "Analytics/functime_wrapper/functime_service.py", {"check_status", "{}"}},
    };

    PythonProcessPool::Config cfg;
    cfg.worker_script = scripts + "/fincept_worker.py";
    cfg.max_workers = 1;
    cfg.ready_timeout_ms = 60'000;

    QTextStream out(stdout);
    out << QString("%1 %2 %3 %4 %5  same\n")
               .arg("call", -62)
               .arg("fresh ms", 9)
               .arg("1st pool", 9)
               .arg("repeat", 8)
               .arg("speed-up", 8);
    for (const Case& c : cases) {
        const QString py = QDir(venvs).filePath(QString(c.venv) + "/" + py_rel);
        if (!QFileInfo::exists(py)) {
            out << c.script << ": no interpreter at " << py << "\n";
            continue;
        }
        Fresh best;
        for (int i = 0; i < 3; ++i) {
            Fresh f = run_fresh(py, scripts, c);
            if (i == 0 || f.ms < best.ms)
                best = f;
        }
        PythonProcessPool pool(cfg); // a new worker: the first call pays its start and imports
        const Pooled first = run_pooled(pool, py, scripts, c);
        std::vector<double> rep;
        Pooled last;
        for (int i = 0; i < 5; ++i) {
            last = run_pooled(pool, py, scripts, c);
            rep.push_back(last.ms);
        }
        std::sort(rep.begin(), rep.end());
        const double median = rep[rep.size() / 2];
        const bool same = last.out.status == PythonProcessPool::Status::Completed &&
                          last.out.exit_code == best.exit_code &&
                          last_line(last.out.stdout_text) == last_line(best.out) &&
                          last_line(first.out.stdout_text) == last_line(best.out);
        const QString label = QString(c.script).section('/', -1) + " " + c.args.first();
        out << QString("%1 %2 %3 %4 %5x  %6\n")
                   .arg(label, -62)
                   .arg(best.ms, 9, 'f', 0)
                   .arg(first.ms, 9, 'f', 0)
                   .arg(median, 8, 'f', 1)
                   .arg(best.ms / median, 7, 'f', 0)
                   .arg(same ? "yes" : "NO");
        if (!same)
            out << "    fresh : " << last_line(best.out).left(160) << "\n    pooled: "
                << last_line(last.out.stdout_text).left(160) << "\n    stderr: " << last.out.stderr_text.right(300)
                << "\n";
        out.flush();
        pool.shutdown();
    }
    return 0;
}
