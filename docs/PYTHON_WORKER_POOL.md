# A Python worker pool for the AI Quant Lab

The terminal runs its Python analytics as child processes: `python <script>
<command> <json>`, one process per call, JSON on stdout. Every call therefore pays
for the interpreter and all of the script's imports before it does any work. For the
AI Quant Lab modules (qlib, PyTorch, statsmodels, scipy) that cost dominates:

| AI Quant Lab script, `check_status` | Fresh process |
| --- | --- |
| `qlib_service.py` (factor discovery, model library, live signals) | 17.9 s |
| `qlib_meta_learning.py` | 14.9 s |
| `qlib_advanced_models.py` | 13.2 s |
| `qlib_rolling_retraining.py` | 9.0 s |
| `qlib_strategy.py`, `qlib_online_learning.py` | 5.6–5.8 s |
| statsmodels, functime, evaluation, portfolio optimisation, ... | 1.8–4.9 s |
| the interpreter alone (`python -c pass`), venv-numpy2 | 0.9 s |

(Ryzen 9 8945HX, Windows 11, the app's own venvs; best of 3.) `check_status`
does nothing but report which libraries are available, so these are almost pure
start-up costs. 0.8 s of the bare interpreter's 0.9 s is one `.pth` file in the venv
(`pip_system_certs.pth`) importing pip's network stack at every start.

This change keeps a few Python processes alive and runs the calls in them, so a
script's imports are paid once per worker instead of once per call.

## What a call sees

`scripts/fincept_worker.py` runs one request at a time and reproduces a fresh
process for each, so no script needed changing:

- `sys.argv`, `__name__ == "__main__"` (via `runpy`), and `sys.path[0]` set as
  `python <script>` or `python -m <module>` would set them;
- stdout and stderr captured separately; the exit code taken from `SystemExit`; an
  uncaught exception prints its traceback and exits 1, as Python does;
- the request's environment (built per call, so credentials changed in the app since
  the worker started are seen), working directory and stdin;
- afterwards: `os.environ`, the working directory, `sys.path` and the warning
  filters put back, and **the scripts tree's own modules dropped from
  `sys.modules`**. Only modules from outside the tree (site-packages, the standard
  library) stay imported. This matters here: `Analytics/functime_wrapper/` and
  `Analytics/gluonts_wrapper/` each have a `core.py`, and a cached `core` would hand
  the second script the first one's code.

The frames (a 4-byte length, then JSON) travel on private duplicates of the pipes.
The worker points file descriptor 1 at stderr and 0 at the null device, so a C
extension or a child process writing to "stdout" cannot corrupt the stream (a test
writes bytes that look like a frame header straight to fd 1).

What it does not reproduce: a script calling `os._exit()` takes the worker down (the
call is reported as failed), and threads a script leaves running keep running until
the worker is recycled. So pooling is opt-in per call (`RunOptions::reuse_process`),
and only the AI Quant Lab's module scripts, which print one result and return, use it.
The deep-research agent, which streams and runs for minutes, keeps its own process.

## The pool

`src/python/PythonProcessPool.{h,cpp}`, QtCore only (no app singletons), so the
tests drive it against a real interpreter:

- **Keyed by interpreter** (+ working directory + `PYTHONPATH`): the numpy-1 and
  numpy-2 venvs get separate workers, as `PythonRunner` routes scripts between them.
- **Up to 2 workers per key**, one request each. A job goes to an idle worker that
  has already run its script if there is one, else any idle worker (its numpy/pandas
  are shared by most scripts), else a new worker if there is room, else it queues.
- **Exactly one callback per job**, always queued to the event loop, never from
  inside `submit()`: callers often submit the next call from the callback.
- **No retries.** A worker that dies or overruns the call's deadline mid-call is
  killed, and the call is reported failed: it may already have trained and saved a
  model. The deadline covers running, not time queued, as for a spawned process.
- **Unavailable, not failed,** when the pool cannot serve a call: no worker could
  start (missing interpreter, a worker that never reports ready), or the call has
  waited 5 s behind busy workers (a long training run holding both). `PythonRunner`
  then runs that call the old way, so the worst case is the old behaviour plus 5 s.
  After a failed start the pool answers Unavailable for a minute instead of trying
  again on every call.
- **Recycling:** after 100 calls, when a worker's resident memory passes 2 GB after
  a call, or after 5 minutes idle. Resident, not committed memory: see
  [Memory](#memory-resident-vs-committed) below.
- **Warm-up:** opening a module in the AI Quant Lab asks a worker to import its
  script (run under another `__name__`, so `main()` does not run) while the user
  fills in the panel. Real calls always go before warm-ups, and a warm-up for a
  script that already has a real call queued is dropped.
- Handlers find their worker by id, never through a captured pointer: `finished()`
  is delivered queued and can arrive after the worker was removed.

`FINCEPT_PYTHON_POOL=off` in the environment turns the pool off.

## Results

### Repeat calls

`fincept-qt/tests/bench_python_pool.cpp` makes real AI Quant Lab calls, with inputs,
three ways: in a fresh process as before (best of 3), as the first call in a new
worker, and repeated in that worker (median of 5). Each pooled call's stdout is
compared byte for byte with the fresh process's.

| Call | Fresh process | First call in a new worker | Repeat in the worker | Speed-up | Same output |
| --- | --- | --- | --- | --- | --- |
| `qlib_portfolio_opt.py` minimum variance | 2.91 s | 2.79 s | 14.7 ms | 198× | yes |
| `qlib_portfolio_opt.py` hierarchical risk parity | 2.78 s | 2.81 s | 10.3 ms | 269× | yes |
| `qlib_evaluation.py` risk metrics | 4.31 s | 4.05 s | 10.1 ms | 425× | yes |
| `statsmodels_service.py` descriptive statistics | 4.41 s | 4.34 s | 9.7 ms | 455× | yes |
| `functime_service.py` `check_status` | 4.49 s | 4.49 s | 9.3 ms | 485× | yes |
| `qlib_meta_learning.py` `check_status` | 13.6 s | 14.4 s | 14.2 ms | 958× | yes |
| `qlib_service.py` `check_status` | 17.1 s | 16.5 s | 29.7 ms | 578× | yes |

The first call in a new worker costs what a fresh process costs: the imports still
have to happen once. In the app the warm-up pays them while the panel is being
filled in. Fresh-process times depend on what else the machine is doing: an earlier
run with other work going on measured 5.0–31.9 s for the same calls, while the
repeats stayed at 13–47 ms.

### In the app

Release build, AI Quant Lab → Statsmodels → OLS regression with the sample data,
from the app's log:

| | |
| --- | --- |
| Opening AI Quant Lab | a worker is ready in 2.0 s and imports the default module's script (`qlib_service.py`) in the background: 17.4 s |
| Switching to Statsmodels | its script is imported in 1.3 s: pandas and scipy are already loaded |
| RUN OLS REGRESSION, twice | 33 ms, then 18 ms in the worker; both on the warm worker |

## Memory: resident vs committed

The first version recycled a worker when its private (committed) memory passed
2 GB. In the benchmark statsmodels and qlib then got no faster at all: their workers
were retired after every call. Committed memory after the imports, in venv-numpy2:

| After importing | Resident | Committed |
| --- | --- | --- |
| `statsmodels.api` | 189 MB | 5.6 GB |
| `statsmodels.api`, `OPENBLAS_NUM_THREADS=4` | 183 MB | 0.72 GB |
| `qlib_service.py` (a worker in the app) | 384 MB | 9.9 GB |

numpy, scipy and cvxopt each ship their own OpenBLAS, and each reserves buffers for
every thread it may use (24, 24 and 32 on this 32-thread CPU) when it loads. The
memory is reserved against the commit limit (RAM + page file) but never touched.

So the pool recycles on resident memory, which is what actually crowds the machine.
Commit still counts against the system limit: on this machine (31 GB of RAM, a
79 GB commit limit) four idle qlib workers fit, on a 16 GB laptop they might not.
Two things keep that in check: idle workers go after 5 minutes, and
`OPENBLAS_NUM_THREADS` cuts the reservation by 8×. The pool does not set it, as it
would change the thread count the scripts compute with compared with a fresh
process; set it in the environment and both paths get it.

## Tests

`fincept-qt/tests/tst_python_process_pool.cpp`: 18 cases against the real worker and
a real interpreter (`FINCEPT_TEST_PYTHON`, else `python3`/`python` on `PATH`, skipping
the Microsoft Store stubs). Passing with the app's Python 3.11 venvs, 3.12 and 3.14,
and 20 repeated runs in a row.

- **A call behaves as in a fresh process:** output, exit codes from `sys.exit()`
  and uncaught exceptions, module form with relative imports, environment and
  working directory per call, 8 MB of output, bytes written straight to fd 1.
- **Modules:** outside modules stay imported (a module on `PYTHONPATH` counts its
  imports); the tree's own are re-imported (two scripts with their own `core.py`).
- **Failures stay contained:** a hung script times out and only its job fails, a
  crash fails only its job, a worker that cannot start or never reports ready makes
  the pool Unavailable (with the cool-down), a job stuck behind busy workers
  overflows to Unavailable, `shutdown()` answers every job exactly once.
- **Capacity:** two workers run in parallel; recycling after N calls, past the
  memory limit, and when idle; warm-up imports without running `main()`.

Each deliberately planted bug below was applied on its own, rebuilt, and the suite
run against it; 9 of the 11 make it fail:

| Planted bug | Caught by |
| --- | --- |
| Worker keeps the tree's own modules cached | `keeps_outside_modules_and_reimports_the_trees_own` |
| Worker keeps fd 1 as the frame stream | `stray_writes_to_fd_1_do_not_corrupt_the_stream` |
| Worker leaves the environment as the script left it | survived (see below) |
| Worker does not restore `sys.path` | survived (see below) |
| No request deadline | `a_hung_script_times_out_and_only_its_job_fails` |
| A timed-out job reported Completed | `a_hung_script_times_out_and_only_its_job_fails` |
| Callbacks delivered synchronously from `submit()` | `unavailable_when_no_worker_can_start` |
| No cool-down after a failed start | `unavailable_when_no_worker_can_start` |
| Recycle one request late | `recycles_after_max_requests` |
| Idle workers never retired | `retires_idle_workers` |
| Queued jobs never overflow | `a_job_stuck_behind_busy_workers_overflows` |

The two survivors are cleanups that are also done at the start of every call: before
the script runs, the environment is replaced with the request's and `sys.path` is
rebuilt from the worker's start-up path, so leaving out the restore afterwards
changes nothing a script can observe.
They stay as defence in depth: the protocol lets a request leave out its environment,
and such a request would otherwise inherit the previous call's changes.

## Limits

- The pool lives in the app process; it is not shared between two running terminals.
- Only the AI Quant Lab uses it. Other Python callers in the app (data fetchers,
  agents) still spawn per call; they could opt in with `reuse_process` after the same
  check that they print one result and return.
- Measured on Windows. The worker reads memory from `/proc` elsewhere and the tests
  are portable, but the numbers above are from one Windows machine.
