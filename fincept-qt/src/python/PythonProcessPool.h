#pragma once
// Long-lived Python workers for PythonRunner (scripts/fincept_worker.py).
//
// A fresh `python <script> <command> <json>` pays for its imports on every call:
// measured on the AI Quant Lab module scripts, 1.8 s for the lightest and 18 s
// for qlib_service.py (qlib + torch), against 6-30 ms for the same call in a
// worker that has already imported them. The pool keeps a few such workers per
// interpreter and hands each one request at a time.
//
// The worker reproduces a fresh process for each request (argv, __main__,
// separate stdout/stderr, exit code, environment, working directory, and the
// scripts tree's own modules re-imported), so a script needs no changes to be
// run here. What it cannot reproduce: a script that calls os._exit() takes the
// worker down (reported as Failed), and threads a script leaves behind keep
// running until the worker is recycled.
//
// Guarantees:
//   * every submitted job gets exactly one callback, on this object's thread,
//     never from inside submit();
//   * a job is never sent to a second worker: one that dies or times out
//     mid-request is reported Failed (it may have written files, trained and
//     saved a model, ...), not retried;
//   * a job that never reached a worker because none could start is reported
//     Unavailable, so the caller can run it the old way. After a failed start
//     the pool answers Unavailable for that interpreter for a cool-down period
//     instead of trying again on every call;
//   * a job that has waited max_queue_wait_ms for a worker (every one busy,
//     say with an hour-long training run) is answered Unavailable too, so a
//     quick call is never stuck behind a long one: at worst it costs what it
//     cost before the pool, plus that wait;
//   * a worker is retired after max_requests_per_worker requests, when its
//     resident memory passes max_worker_mb, or after idle_timeout_ms unused.
//
// QtCore only (no app logger, no PythonSetupManager): PythonRunner resolves the
// interpreter, environment and script path and passes them in, which is what
// lets tests/tst_python_process_pool.cpp drive it against a real interpreter.

#include <QHash>
#include <QObject>
#include <QProcess>
#include <QProcessEnvironment>
#include <QString>
#include <QStringList>

#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

class QTimer;

namespace fincept::python {

class PythonProcessPool : public QObject {
    Q_OBJECT
  public:
    struct Config {
        /// Absolute path of scripts/fincept_worker.py.
        QString worker_script;
        /// Directory whose modules each request re-imports (passed to the worker as
        /// --tree). Empty: the worker script's own directory.
        QString tree;
        /// Workers per pool key (interpreter + working directory + PYTHONPATH).
        int max_workers = 2;
        int max_requests_per_worker = 100;
        /// Resident memory (working set / RSS) a worker may report after a request
        /// before it is retired. Not commit: an idle worker with numpy, scipy and
        /// cvxopt loaded commits 5-10 GB that it never touches (three OpenBLAS
        /// copies, buffers per thread), which would retire it after every call.
        double max_worker_mb = 2048;
        /// Idle workers are retired after this: a qlib worker holds ~0.5 GB of RAM
        /// and ~10 GB of commit.
        int idle_timeout_ms = 5 * 60 * 1000;
        /// From process start to the worker's ready frame.
        int ready_timeout_ms = 30'000;
        /// After a worker failed to start, answer Unavailable this long.
        int unavailable_cooldown_ms = 60'000;
        /// After closing a retiring worker's stdin, kill it if it has not exited.
        int retire_grace_ms = 5'000;
        /// A job still queued after this long is answered Unavailable (run it as
        /// its own process). 0: wait however long it takes.
        int max_queue_wait_ms = 5'000;
        qint64 max_frame_bytes = qint64{1} << 30;
        std::function<void(const QString&)> log;
    };

    struct Job {
        QString interpreter;
        QString working_dir;
        QProcessEnvironment env;
        QString script; ///< absolute path of the script, or
        QString module; ///< a dotted module name (the `python -m` form)
        QStringList args;
        QByteArray stdin_data;
        /// Budget for running the job once a worker has it (time queued behind
        /// other jobs does not count, as with a spawned process). 0 = none.
        int timeout_ms = 0;
    };

    enum class Status {
        Completed,   ///< the script ran; exit_code, stdout, stderr are its own
        Failed,      ///< the worker died or timed out while running it
        Unavailable, ///< no worker could take it: run it another way
    };

    struct Outcome {
        Status status = Status::Unavailable;
        int exit_code = -1;
        QString stdout_text;
        QString stderr_text;
        QString error; ///< why, for Failed / Unavailable
        bool timed_out = false;
        qint64 worker_pid = 0;
        /// The worker had already run or warmed this script: its imports were cached.
        bool warm = false;
        double run_ms = 0; ///< inside the worker
    };

    using Callback = std::function<void(Outcome)>;

    struct Stats {
        int workers = 0; ///< alive, any state
        int starting = 0;
        int busy = 0;
        int queued = 0;
        quint64 spawned = 0;
        quint64 completed = 0;
        quint64 failed = 0;
        quint64 unavailable = 0;
        quint64 overflowed = 0; ///< answered Unavailable after max_queue_wait_ms
        quint64 timed_out = 0;
        quint64 retired = 0;
        quint64 warm_hits = 0;
    };

    explicit PythonProcessPool(Config cfg, QObject* parent = nullptr);
    /// Kills the workers. Callbacks still owed are dropped, not called: by the
    /// time the pool is destroyed their receivers may be gone. Call shutdown()
    /// first to have them answered.
    ~PythonProcessPool() override;

    void submit(Job job, Callback cb);

    /// Import `job`'s script in a worker without running its main, so that the
    /// first real call skips the imports. Starts a worker if there is room. A
    /// no-op when a worker of that key already has the script, or a warm-up of
    /// it is already queued. Real jobs are always served before warm-ups.
    void warm(Job job);

    /// Kill every worker; answer every job still owed (Failed if it was running,
    /// Unavailable if it was queued). Later submits answer Unavailable.
    void shutdown();

    Stats stats() const;

  private:
    struct Pending {
        quint64 id = 0;
        Job job;
        Callback cb;
        bool warm_only = false;
        qint64 queued_ms = 0;
    };

    enum class State { Starting, Idle, Busy, Retiring };

    struct Worker {
        quint64 id = 0;
        QString key;
        QProcess* proc = nullptr;
        QTimer* timer = nullptr; ///< ready timeout, request deadline or retire grace
        State state = State::Starting;
        QByteArray rx;
        QByteArray stderr_tail;
        qint64 pid = 0;
        int served = 0;
        QStringList warm_for; ///< scripts / modules imported here
        std::optional<Pending> job;
        bool job_was_warm = false;
        bool timed_out = false;
        bool killed = false; ///< by us, for a protocol error: ignore what it still sends
        bool reported_memory = false;
        bool gone = false;   ///< exited; being removed
        QString kill_reason;
        qint64 started_ms = 0;
        qint64 idle_since_ms = 0;
    };

    struct Key {
        std::deque<Pending> jobs;
        std::deque<Pending> warms;
        std::vector<std::unique_ptr<Worker>> workers;
        qint64 unavailable_until_ms = 0;
        QString unavailable_reason;
    };

    static QString key_of(const Job& job);
    static QString target_of(const Job& job);
    void log(const QString& line) const;
    void pump(const QString& key);
    void spawn(Key& k, const QString& key, const Job& like);
    void dispatch(Worker* w, Pending p);
    void retire(Worker* w, const QString& why);
    void on_stdout(Worker* w);
    void on_stderr(Worker* w);
    void on_frame(Worker* w, const QByteArray& body);
    void on_gone(Worker* w, const QString& why);
    void protocol_error(Worker* w, const QString& why);
    void sweep_idle();
    void overflow_queued();
    /// Remove `w` from its key and delete it. Returns the job it held, if any.
    std::optional<Pending> remove(Worker* w);
    void answer_later(Callback cb, Outcome o);

    Config cfg_;
    std::unordered_map<QString, Key> keys_; // Key is move-only: no QHash
    /// Signal handlers find their worker through its id, never a captured pointer:
    /// finished() is delivered queued and may arrive after the worker was removed.
    QHash<quint64, Worker*> by_id_;
    quint64 next_worker_id_ = 1;
    QTimer* idle_timer_ = nullptr;
    QTimer* queue_timer_ = nullptr; ///< runs while jobs are queued
    quint64 next_id_ = 1;
    bool shut_down_ = false;
    Stats counters_;
};

} // namespace fincept::python
