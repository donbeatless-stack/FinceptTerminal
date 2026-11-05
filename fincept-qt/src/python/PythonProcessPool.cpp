#include "python/PythonProcessPool.h"

#include <QDateTime>
#include <QFileInfo>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QTimer>
#include <QtEndian>

#include <algorithm>

namespace fincept::python {

namespace {

qint64 now_ms() {
    return QDateTime::currentMSecsSinceEpoch();
}

constexpr qsizetype kStderrKeep = 64 * 1024;

QString tail(const QByteArray& bytes, int max_chars = 2000) {
    return QString::fromUtf8(bytes.right(max_chars)).trimmed();
}

} // namespace

PythonProcessPool::PythonProcessPool(Config cfg, QObject* parent) : QObject(parent), cfg_(std::move(cfg)) {
    idle_timer_ = new QTimer(this);
    idle_timer_->setInterval(std::clamp(cfg_.idle_timeout_ms / 4, 250, 30'000));
    connect(idle_timer_, &QTimer::timeout, this, &PythonProcessPool::sweep_idle);
    queue_timer_ = new QTimer(this);
    queue_timer_->setInterval(std::clamp(cfg_.max_queue_wait_ms / 5, 50, 1'000));
    connect(queue_timer_, &QTimer::timeout, this, &PythonProcessPool::overflow_queued);
}

PythonProcessPool::~PythonProcessPool() {
    for (auto& [key, k] : keys_) {
        for (auto& w : k.workers) {
            w->proc->disconnect(this);
            w->timer->disconnect(this);
            if (w->proc->state() != QProcess::NotRunning) {
                w->proc->kill();
                w->proc->waitForFinished(2'000); // the children are deleted next
            }
        }
    }
}

QString PythonProcessPool::key_of(const Job& job) {
    return job.interpreter + QLatin1Char('\n') + job.working_dir + QLatin1Char('\n') +
           job.env.value(QStringLiteral("PYTHONPATH"));
}

QString PythonProcessPool::target_of(const Job& job) {
    return job.module.isEmpty() ? job.script : QStringLiteral("-m ") + job.module;
}

void PythonProcessPool::log(const QString& line) const {
    if (cfg_.log)
        cfg_.log(line);
}

void PythonProcessPool::answer_later(Callback cb, Outcome o) {
    if (!cb)
        return;
    // Never from inside submit(), and never while a handler is half-way through
    // updating the pool: the callback may well submit the next job.
    QMetaObject::invokeMethod(
        this, [cb = std::move(cb), o = std::move(o)]() mutable { cb(std::move(o)); }, Qt::QueuedConnection);
}

void PythonProcessPool::submit(Job job, Callback cb) {
    const QString key = key_of(job);
    Key& k = keys_[key];
    if (shut_down_ || now_ms() < k.unavailable_until_ms) {
        ++counters_.unavailable;
        Outcome o;
        o.status = Status::Unavailable;
        o.error = shut_down_ ? QStringLiteral("Python worker pool is shut down") : k.unavailable_reason;
        answer_later(std::move(cb), std::move(o));
        return;
    }
    // A queued warm-up of the same script is now pointless: this job imports it.
    const QString target = target_of(job);
    k.warms.erase(std::remove_if(k.warms.begin(), k.warms.end(),
                                 [&](const Pending& p) { return target_of(p.job) == target; }),
                  k.warms.end());
    k.jobs.push_back({next_id_++, std::move(job), std::move(cb), false, now_ms()});
    pump(key);
    if (!keys_[key].jobs.empty() && cfg_.max_queue_wait_ms > 0 && !queue_timer_->isActive())
        queue_timer_->start();
}

void PythonProcessPool::warm(Job job) {
    if (shut_down_)
        return;
    const QString key = key_of(job);
    Key& k = keys_[key];
    if (now_ms() < k.unavailable_until_ms)
        return;
    const QString target = target_of(job);
    for (const auto& w : k.workers) {
        if (w->gone || w->state == State::Retiring)
            continue;
        if (w->warm_for.contains(target) || (w->job && target_of(w->job->job) == target))
            return;
    }
    for (const auto& p : k.warms)
        if (target_of(p.job) == target)
            return;
    for (const auto& p : k.jobs)
        if (target_of(p.job) == target)
            return;
    k.warms.push_back({next_id_++, std::move(job), {}, true});
    pump(key);
}

void PythonProcessPool::pump(const QString& key) {
    auto it = keys_.find(key);
    if (it == keys_.end())
        return;
    Key& k = it->second;

    auto idle_worker = [&k](const QString& prefer) -> Worker* {
        Worker* any = nullptr;
        for (auto& w : k.workers) {
            if (w->gone || w->state != State::Idle)
                continue;
            if (w->warm_for.contains(prefer))
                return w.get();
            if (!any)
                any = w.get();
        }
        return any;
    };

    // Real jobs first, each to an idle worker that already has its imports if
    // there is one. Any idle worker beats starting a new one: whatever it has
    // imported (numpy, pandas, ...) is shared by most scripts.
    while (!k.jobs.empty()) {
        Worker* w = idle_worker(target_of(k.jobs.front().job));
        if (!w)
            break;
        Pending p = std::move(k.jobs.front());
        k.jobs.pop_front();
        dispatch(w, std::move(p));
    }
    // Warm-ups only to workers no job wants.
    while (k.jobs.empty() && !k.warms.empty()) {
        Pending p = std::move(k.warms.front());
        k.warms.pop_front();
        const QString target = target_of(p.job);
        const bool done = std::any_of(k.workers.begin(), k.workers.end(), [&](const auto& w) {
            return !w->gone && w->state != State::Retiring && w->warm_for.contains(target);
        });
        if (done)
            continue;
        Worker* w = idle_worker(QString());
        if (!w) {
            k.warms.push_front(std::move(p));
            break;
        }
        dispatch(w, std::move(p));
    }

    if (shut_down_ || now_ms() < k.unavailable_until_ms)
        return;
    int live = 0;
    int starting = 0;
    for (const auto& w : k.workers) {
        if (w->gone || w->state == State::Retiring)
            continue;
        ++live;
        if (w->state == State::Starting)
            ++starting;
    }
    const auto waiting = static_cast<int>(k.jobs.size() + k.warms.size());
    while (waiting > starting && live < cfg_.max_workers) {
        spawn(k, key, !k.jobs.empty() ? k.jobs.front().job : k.warms.front().job);
        ++live;
        ++starting;
    }
}

void PythonProcessPool::spawn(Key& k, const QString& key, const Job& like) {
    auto owned = std::make_unique<Worker>();
    Worker* w = owned.get();
    const quint64 wid = next_worker_id_++;
    w->id = wid;
    w->key = key;
    w->proc = new QProcess(this);
    w->timer = new QTimer(w->proc);
    w->timer->setSingleShot(true);
    k.workers.push_back(std::move(owned));
    by_id_.insert(wid, w);
    ++counters_.spawned;

    QProcess* proc = w->proc;
    auto find = [this, wid]() -> Worker* { return by_id_.value(wid, nullptr); };

    connect(proc, &QProcess::readyReadStandardOutput, this, [this, find]() {
        if (Worker* x = find())
            on_stdout(x);
    });
    connect(proc, &QProcess::readyReadStandardError, this, [this, find]() {
        if (Worker* x = find())
            on_stderr(x);
    });
    // Queued: QProcess may report FailedToStart from inside start(), i.e. from
    // inside pump(), which is iterating this key's workers.
    connect(
        proc, &QProcess::errorOccurred, this,
        [this, find](QProcess::ProcessError e) {
            if (e != QProcess::FailedToStart)
                return; // Crashed: finished() follows; read/write errors are not terminal
            if (Worker* x = find())
                on_gone(x, QStringLiteral("could not start: ") + x->proc->errorString());
        },
        Qt::QueuedConnection);
    connect(
        proc, &QProcess::finished, this,
        [this, find](int code, QProcess::ExitStatus status) {
            if (Worker* x = find())
                on_gone(x, status == QProcess::CrashExit ? QStringLiteral("crashed or was killed")
                                                         : QStringLiteral("exited with code %1").arg(code));
        },
        Qt::QueuedConnection);
    connect(w->timer, &QTimer::timeout, this, [this, find]() {
        Worker* x = find();
        if (!x || x->gone)
            return;
        switch (x->state) {
        case State::Starting:
            x->kill_reason = QStringLiteral("did not report ready within %1 s").arg(cfg_.ready_timeout_ms / 1000.0);
            x->proc->kill();
            break;
        case State::Busy:
            x->timed_out = true;
            x->proc->kill();
            break;
        case State::Retiring:
            x->proc->kill();
            break;
        case State::Idle:
            break;
        }
    });

    proc->setProgram(like.interpreter);
    QStringList args{cfg_.worker_script};
    if (!cfg_.tree.isEmpty())
        args << QStringLiteral("--tree") << cfg_.tree;
    proc->setArguments(args);
    proc->setWorkingDirectory(like.working_dir);
    proc->setProcessEnvironment(like.env);
#ifdef Q_OS_WIN
    proc->setCreateProcessArgumentsModifier([](QProcess::CreateProcessArguments* cpa) {
        cpa->flags |= 0x08000000; // CREATE_NO_WINDOW
    });
#endif
    w->started_ms = now_ms();
    w->timer->start(cfg_.ready_timeout_ms);
    if (!idle_timer_->isActive())
        idle_timer_->start();
    log(QStringLiteral("starting a worker for %1").arg(like.interpreter));
    proc->start();
}

void PythonProcessPool::dispatch(Worker* w, Pending p) {
    QJsonObject req;
    req[QStringLiteral("id")] = static_cast<double>(p.id);
    if (p.job.module.isEmpty())
        req[QStringLiteral("script")] = p.job.script;
    else
        req[QStringLiteral("module")] = p.job.module;
    req[QStringLiteral("argv")] = QJsonArray::fromStringList(p.job.args);
    req[QStringLiteral("cwd")] = p.job.working_dir;
    QJsonObject env;
    for (const QString& name : p.job.env.keys()) {
        // Windows' hidden per-drive working directories ("=C:"): not variables
        // a script can see, and putenv rejects them.
        if (name.isEmpty() || name.startsWith(QLatin1Char('=')))
            continue;
        env[name] = p.job.env.value(name);
    }
    req[QStringLiteral("env")] = env;
    if (!p.job.stdin_data.isEmpty())
        req[QStringLiteral("stdin")] = QString::fromUtf8(p.job.stdin_data);
    if (p.warm_only)
        req[QStringLiteral("warm")] = true;

    const QByteArray body = QJsonDocument(req).toJson(QJsonDocument::Compact);
    QByteArray frame(4, Qt::Uninitialized);
    qToBigEndian<quint32>(static_cast<quint32>(body.size()), frame.data());
    frame += body;

    w->job_was_warm = w->warm_for.contains(target_of(p.job));
    w->timed_out = false;
    w->state = State::Busy;
    if (p.job.timeout_ms > 0)
        w->timer->start(p.job.timeout_ms);
    else
        w->timer->stop();
    w->job = std::move(p);
    w->proc->write(frame);
}

void PythonProcessPool::retire(Worker* w, const QString& why) {
    if (w->gone || w->state == State::Retiring)
        return;
    w->state = State::Retiring;
    ++counters_.retired;
    log(QStringLiteral("retiring worker %1: %2").arg(w->pid).arg(why));
    w->proc->closeWriteChannel(); // EOF: the worker returns from its loop and exits
    w->timer->start(cfg_.retire_grace_ms);
}

void PythonProcessPool::on_stderr(Worker* w) {
    w->stderr_tail += w->proc->readAllStandardError();
    if (w->stderr_tail.size() > 2 * kStderrKeep)
        w->stderr_tail.remove(0, w->stderr_tail.size() - kStderrKeep);
}

void PythonProcessPool::on_stdout(Worker* w) {
    w->rx += w->proc->readAllStandardOutput();
    while (!w->killed && w->rx.size() >= 4) {
        const quint32 n = qFromBigEndian<quint32>(w->rx.constData());
        if (n > cfg_.max_frame_bytes) {
            protocol_error(w, QStringLiteral("frame of %1 bytes").arg(n));
            return;
        }
        if (w->rx.size() < 4 + static_cast<qsizetype>(n))
            return;
        const QByteArray body = w->rx.mid(4, n);
        w->rx.remove(0, 4 + n);
        on_frame(w, body);
    }
}

void PythonProcessPool::protocol_error(Worker* w, const QString& why) {
    log(QStringLiteral("worker %1: protocol error: %2").arg(w->pid).arg(why));
    w->kill_reason = QStringLiteral("protocol error: ") + why;
    w->killed = true;
    w->proc->kill(); // finished() then fails whatever it was running
}

void PythonProcessPool::on_frame(Worker* w, const QByteArray& body) {
    QJsonParseError pe{};
    const QJsonDocument doc = QJsonDocument::fromJson(body, &pe);
    if (pe.error != QJsonParseError::NoError || !doc.isObject()) {
        protocol_error(w, QStringLiteral("unreadable frame (%1)").arg(pe.errorString()));
        return;
    }
    const QJsonObject obj = doc.object();

    if (w->state == State::Starting) {
        if (!obj.value(QStringLiteral("ready")).toBool()) {
            protocol_error(w, QStringLiteral("expected the ready frame"));
            return;
        }
        w->pid = obj.value(QStringLiteral("pid")).toInteger();
        w->state = State::Idle;
        w->timer->stop();
        w->idle_since_ms = now_ms();
        keys_[w->key].unavailable_until_ms = 0;
        log(QStringLiteral("worker %1 ready in %2 ms").arg(w->pid).arg(now_ms() - w->started_ms));
        pump(w->key);
        return;
    }

    if (w->state != State::Busy || !w->job ||
        obj.value(QStringLiteral("id")).toInteger(-1) != static_cast<qint64>(w->job->id)) {
        protocol_error(w, QStringLiteral("reply for no request in flight"));
        return;
    }

    Pending p = std::move(*w->job);
    w->job.reset();
    w->timer->stop();
    w->state = State::Idle;
    w->idle_since_ms = now_ms();
    ++w->served;
    const QString target = target_of(p.job);
    if (!w->warm_for.contains(target))
        w->warm_for << target;

    Outcome o;
    o.status = Status::Completed;
    o.exit_code = obj.value(QStringLiteral("exit_code")).toInt(1);
    o.stdout_text = obj.value(QStringLiteral("stdout")).toString();
    o.stderr_text = obj.value(QStringLiteral("stderr")).toString();
    o.worker_pid = w->pid;
    o.warm = w->job_was_warm;
    o.run_ms = obj.value(QStringLiteral("ms")).toDouble();

    const double mb = obj.value(QStringLiteral("rss_mb")).toDouble(-1);
    if (!w->reported_memory) {
        w->reported_memory = true;
        log(QStringLiteral("worker %1 after its first call: %2 MB resident, %3 MB committed")
                .arg(w->pid)
                .arg(mb, 0, 'f', 0)
                .arg(obj.value(QStringLiteral("commit_mb")).toDouble(-1), 0, 'f', 0));
    }
    if (w->served >= cfg_.max_requests_per_worker)
        retire(w, QStringLiteral("served %1 requests").arg(w->served));
    else if (mb > cfg_.max_worker_mb)
        retire(w, QStringLiteral("using %1 MB").arg(mb, 0, 'f', 0));

    if (p.warm_only) {
        log(QStringLiteral("worker %1 warmed %2 in %3 ms").arg(w->pid).arg(target).arg(o.run_ms, 0, 'f', 0));
    } else {
        ++counters_.completed;
        if (o.warm)
            ++counters_.warm_hits;
        answer_later(std::move(p.cb), std::move(o));
    }
    pump(w->key);
}

void PythonProcessPool::on_gone(Worker* w, const QString& why) {
    if (w->gone)
        return;
    // Marked first: a reply that arrived together with the exit is still
    // delivered below, but pump() must not hand this worker another job.
    w->gone = true;
    if (!w->killed)
        on_stdout(w);
    on_stderr(w);

    const State was = w->state;
    const QString key = w->key;
    const bool timed_out = w->timed_out;
    const qint64 pid = w->pid;
    const QString reason = w->kill_reason.isEmpty() ? why : w->kill_reason;
    const QString err_tail = tail(w->stderr_tail);
    const int budget_ms = w->job ? w->job->job.timeout_ms : 0;
    std::optional<Pending> lost = remove(w);
    Key& k = keys_[key];

    if (was == State::Starting) {
        const bool ready_left = std::any_of(k.workers.begin(), k.workers.end(), [](const auto& x) {
            return !x->gone && (x->state == State::Idle || x->state == State::Busy);
        });
        QString msg = QStringLiteral("Python worker %1").arg(reason);
        if (!err_tail.isEmpty())
            msg += QStringLiteral(": ") + err_tail;
        log(msg);
        if (!ready_left) {
            k.unavailable_until_ms = now_ms() + cfg_.unavailable_cooldown_ms;
            k.unavailable_reason = msg;
            while (!k.jobs.empty()) {
                Pending p = std::move(k.jobs.front());
                k.jobs.pop_front();
                ++counters_.unavailable;
                Outcome o;
                o.status = Status::Unavailable;
                o.error = msg;
                answer_later(std::move(p.cb), std::move(o));
            }
            k.warms.clear();
        }
    } else if (was == State::Busy && lost) {
        if (timed_out)
            ++counters_.timed_out;
        log(QStringLiteral("worker %1 %2 while running %3")
                .arg(pid)
                .arg(timed_out ? QStringLiteral("timed out") : reason)
                .arg(target_of(lost->job)));
        if (!lost->warm_only) {
            ++counters_.failed;
            Outcome o;
            o.status = Status::Failed;
            o.timed_out = timed_out;
            o.worker_pid = pid;
            if (timed_out) {
                o.error = QStringLiteral("Script timed out after %1 s and was killed").arg(budget_ms / 1000);
            } else {
                o.error = QStringLiteral("Python worker %1 while running the script").arg(reason);
                if (!err_tail.isEmpty())
                    o.error += QStringLiteral("\n") + err_tail;
            }
            answer_later(std::move(lost->cb), std::move(o));
        }
    } else if (was != State::Retiring) {
        log(QStringLiteral("idle worker %1 %2").arg(pid).arg(reason));
    }
    pump(key); // replace it if anything is waiting
}

std::optional<PythonProcessPool::Pending> PythonProcessPool::remove(Worker* w) {
    auto it = keys_.find(w->key);
    if (it == keys_.end())
        return std::nullopt;
    auto& workers = it->second.workers;
    auto pos = std::find_if(workers.begin(), workers.end(), [w](const auto& x) { return x.get() == w; });
    if (pos == workers.end())
        return std::nullopt;
    std::unique_ptr<Worker> owned = std::move(*pos);
    workers.erase(pos);
    by_id_.remove(owned->id);

    std::optional<Pending> job = std::move(owned->job);
    owned->timer->stop();
    owned->timer->disconnect(this);
    QProcess* proc = owned->proc;
    proc->disconnect(this);
    if (proc->state() != QProcess::NotRunning) {
        // Delete it once it has really exited, not while it is still running.
        connect(proc, &QProcess::finished, proc, &QObject::deleteLater);
        connect(proc, &QProcess::errorOccurred, proc, [proc](QProcess::ProcessError e) {
            if (e == QProcess::FailedToStart)
                proc->deleteLater();
        });
        proc->kill();
    } else {
        proc->deleteLater();
    }
    return job;
}

void PythonProcessPool::sweep_idle() {
    const qint64 now = now_ms();
    bool any = false;
    for (auto& [key, k] : keys_) {
        for (auto& w : k.workers) {
            if (w->gone)
                continue;
            any = true;
            if (w->state == State::Idle && now - w->idle_since_ms >= cfg_.idle_timeout_ms)
                retire(w.get(), QStringLiteral("idle for %1 s").arg((now - w->idle_since_ms) / 1000));
        }
    }
    if (!any)
        idle_timer_->stop();
}

void PythonProcessPool::overflow_queued() {
    const qint64 now = now_ms();
    bool waiting = false;
    for (auto& [key, k] : keys_) {
        // Oldest first: jobs are only ever appended.
        while (!k.jobs.empty() && now - k.jobs.front().queued_ms >= cfg_.max_queue_wait_ms) {
            Pending p = std::move(k.jobs.front());
            k.jobs.pop_front();
            ++counters_.overflowed;
            Outcome o;
            o.status = Status::Unavailable;
            o.error = QStringLiteral("every Python worker busy for %1 s").arg(cfg_.max_queue_wait_ms / 1000.0);
            answer_later(std::move(p.cb), std::move(o));
        }
        waiting = waiting || !k.jobs.empty();
    }
    if (!waiting)
        queue_timer_->stop();
}

void PythonProcessPool::shutdown() {
    shut_down_ = true;
    idle_timer_->stop();
    queue_timer_->stop();
    for (auto& [key, k] : keys_) {
        while (!k.jobs.empty()) {
            Pending p = std::move(k.jobs.front());
            k.jobs.pop_front();
            ++counters_.unavailable;
            Outcome o;
            o.status = Status::Unavailable;
            o.error = QStringLiteral("Python worker pool is shut down");
            answer_later(std::move(p.cb), std::move(o));
        }
        k.warms.clear();
        while (!k.workers.empty()) {
            Worker* w = k.workers.back().get();
            const qint64 pid = w->pid;
            std::optional<Pending> lost = remove(w);
            if (lost && !lost->warm_only) {
                ++counters_.failed;
                Outcome o;
                o.status = Status::Failed;
                o.worker_pid = pid;
                o.error = QStringLiteral("Python worker pool shut down while the script was running");
                answer_later(std::move(lost->cb), std::move(o));
            }
        }
    }
}

PythonProcessPool::Stats PythonProcessPool::stats() const {
    Stats s = counters_;
    for (const auto& [key, k] : keys_) {
        s.queued += static_cast<int>(k.jobs.size());
        for (const auto& w : k.workers) {
            if (w->gone)
                continue;
            ++s.workers;
            if (w->state == State::Starting)
                ++s.starting;
            else if (w->state == State::Busy)
                ++s.busy;
        }
    }
    return s;
}

} // namespace fincept::python
