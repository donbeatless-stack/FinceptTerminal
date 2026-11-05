"""Pooled script runner behind PythonRunner (see src/python/PythonProcessPool.h).

A fresh `python <script> <command> <json>` pays for its imports on every call:
1.8 s for the lightest AI Quant Lab module, 18 s for qlib_service.py. This
process stays alive and runs those calls one at a time, so third-party imports
(numpy, pandas, scipy, qlib, torch) are paid once per worker, not once per call.

Each request sees what a fresh process would have seen:
  * sys.argv = [script, *args], __name__ == "__main__", sys.path[0] = the
    script's directory (or the working directory for module-form requests);
  * stdout and stderr captured separately, the exit code taken from SystemExit
    (an uncaught exception prints its traceback and exits 1, as Python does);
  * os.environ, the working directory, sys.path and the warning filters put
    back afterwards;
  * the scripts tree's own modules dropped from sys.modules afterwards, so the
    next request imports them afresh. Two directories here each have a
    core.py; a cached one would hand the second script the first one's code.
    Only modules from outside the tree (site-packages, the stdlib) stay warm.

Not reproduced: a script that calls os._exit() takes the worker down with it
(the host reports the request as failed), and threads a script leaves running
keep running. The host recycles workers after a number of requests or when
their resident memory grows past a limit.

Protocol, one request at a time:
  frames  4-byte big-endian length, then that many bytes of UTF-8 JSON
  worker  {"ready": true, "pid": n}
  host    {"id": n, "script": path} or {"id": n, "module": "pkg.mod"}, with
          "argv": [...], optional "env": {...} (replaces os.environ for the
          call), "cwd", "stdin" (text the script reads from sys.stdin), and
          "warm": true to only import the script (run under another __name__,
          so main() does not run) without replying with its output
  worker  {"id": n, "exit_code": int, "stdout": str, "stderr": str,
           "ms": float, "rss_mb": float, "commit_mb": float}
  The host closes the worker's stdin to stop it.

The frames travel on private duplicates of fd 0 and fd 1. fd 1 itself is
pointed at fd 2 and fd 0 at the null device, so a C extension or a child
process that writes to "stdout" or reads "stdin" cannot corrupt the stream.
"""

import io
import json
import os
import runpy
import struct
import sys
import time
import traceback
import warnings

_MAX_FRAME = 1 << 30
_STDERR_KEEP = 1 << 20  # stderr is only an error message: keep its tail


class _Sink(io.RawIOBase):
    """Byte sink behind the per-request sys.stdout / sys.stderr.

    A script may wrap sys.stdout.buffer in its own TextIOWrapper; when that
    wrapper is collected it closes its buffer, so close() must not stick.
    """

    def __init__(self):
        super().__init__()
        self.data = bytearray()

    def writable(self):
        return True

    def write(self, b):
        self.data += b
        return len(b)

    def close(self):
        pass

    @property
    def closed(self):
        return False

    def take(self):
        out = bytes(self.data)
        self.data = bytearray()
        return out


def _text_stream(sink):
    return io.TextIOWrapper(sink, encoding="utf-8", errors="backslashreplace", newline="\n", write_through=True)


def _replace_environ(env):
    """os.environ = env, skipping names the OS will not take. Windows keeps
    per-drive working directories in hidden variables named like "=C:"; they
    show up in a process's environment block, but putenv rejects them."""
    os.environ.clear()
    for k, v in env.items():
        k, v = str(k), str(v)
        if not k or "=" in k[1:] or k.startswith("=") or "\0" in k or "\0" in v:
            continue
        try:
            os.environ[k] = v
        except (ValueError, OSError):
            pass


def _memory_mb():
    """(resident, committed) in MB: the working set and private bytes on Windows,
    RSS and the data segment elsewhere. Only the first is a recycling signal.
    Commit is huge and mostly untouched here: numpy, scipy and cvxopt each ship
    their own OpenBLAS, and each reserves buffers per thread at import, so on a
    32-thread CPU `import statsmodels.api` commits 5.6 GB for 189 MB resident."""
    try:
        if sys.platform == "win32":
            import ctypes
            from ctypes import wintypes

            class PMC(ctypes.Structure):
                _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                            ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaNonPagedPoolUsage", ctypes.c_size_t), ("PagefileUsage", ctypes.c_size_t),
                            ("PeakPagefileUsage", ctypes.c_size_t)]

            pmc = PMC()
            pmc.cb = ctypes.sizeof(PMC)
            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            k32.GetCurrentProcess.restype = wintypes.HANDLE
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD]
            if psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb):
                return pmc.WorkingSetSize / (1 << 20), pmc.PagefileUsage / (1 << 20)
            return -1.0, -1.0
        with open("/proc/self/statm") as f:
            fields = f.read().split()
        page = os.sysconf("SC_PAGE_SIZE") / (1 << 20)
        return int(fields[1]) * page, int(fields[5]) * page
    except Exception:
        return -1.0, -1.0


class Worker:
    def __init__(self, tree):
        self.tree = os.path.normcase(os.path.abspath(tree))
        # Private copies of the pipes, then fd 0/1 pointed somewhere harmless.
        self.proto_in = os.fdopen(os.dup(0), "rb", buffering=0)
        self.proto_out = os.dup(1)
        sys.stdout.flush()
        os.dup2(2, 1)
        null = os.open(os.devnull, os.O_RDONLY)
        os.dup2(null, 0)
        os.close(null)

        self.out_sink, self.err_sink = _Sink(), _Sink()
        self.out, self.err = _text_stream(self.out_sink), _text_stream(self.err_sink)
        # Outside a request: our own diagnostics go to the process's stderr.
        self.real_stderr = sys.__stderr__
        sys.stdin = io.StringIO()
        # sys.path as `python <script>` would build it, minus its first entry
        # (the script's directory), which each request sets for itself.
        self.base_path = list(sys.path[1:])

    # ── framing ──────────────────────────────────────────────────────────────
    def _read_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.proto_in.read(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return bytes(buf)

    def read_frame(self):
        head = self._read_exact(4)
        if head is None:
            return None
        (n,) = struct.unpack(">I", head)
        if n > _MAX_FRAME:
            raise ValueError(f"frame of {n} bytes")
        body = self._read_exact(n)
        if body is None:
            return None
        return json.loads(body.decode("utf-8"))

    def write_frame(self, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8", "backslashreplace")
        data = struct.pack(">I", len(body)) + body
        view = memoryview(data)
        while view:
            n = os.write(self.proto_out, view)
            view = view[n:]

    # ── one request ──────────────────────────────────────────────────────────
    def _ours(self, mod):
        files = []
        f = getattr(mod, "__file__", None)
        if f:
            files.append(f)
        else:
            files.extend(getattr(mod, "__path__", None) or [])
        for p in files:
            try:
                p = os.path.normcase(os.path.abspath(p))
            except Exception:
                continue
            if p == self.tree or p.startswith(self.tree + os.sep):
                return True
        return False

    def _forget_our_modules(self):
        for name, mod in list(sys.modules.items()):
            if mod is not None and name != "__main__" and self._ours(mod):
                del sys.modules[name]

    def run(self, req):
        argv = [str(a) for a in req.get("argv", [])]
        script, module = req.get("script"), req.get("module")
        cwd = req.get("cwd") or os.getcwd()
        warm = bool(req.get("warm"))

        saved_env = dict(os.environ)
        saved_cwd = os.getcwd()
        saved_path = list(sys.path)
        saved_argv = list(sys.argv)
        saved_filters = list(warnings.filters)
        saved_streams = (sys.stdout, sys.stderr, sys.stdin)

        exit_code = 0
        t0 = time.perf_counter()
        try:
            # Capture first, so that anything below that fails is reported in
            # this request's stderr rather than lost in the worker's.
            sys.stdout, sys.stderr = self.out, self.err
            sys.stdin = io.StringIO(req.get("stdin") or "")
            if req.get("env") is not None:
                _replace_environ(req["env"])
            os.chdir(cwd)
            run_name = "__fincept_warm__" if warm else "__main__"
            if module:
                sys.path[:] = [os.path.abspath(cwd)] + self.base_path
                sys.argv = [module] + argv  # run_module puts the file path in argv[0]
                runpy.run_module(module, run_name=run_name, alter_sys=True)
            else:
                path = os.path.abspath(script)
                sys.path[:] = [os.path.dirname(path)] + self.base_path
                sys.argv = [path] + argv
                runpy.run_path(path, run_name=run_name)
        except SystemExit as e:
            code = e.code
            if code is None:
                exit_code = 0
            elif isinstance(code, int):
                exit_code = code
            else:
                print(code, file=sys.stderr)
                exit_code = 1
        except BaseException:
            traceback.print_exc(file=sys.stderr)
            exit_code = 1
        finally:
            for s in (sys.stdout, sys.stderr):
                try:
                    s.flush()
                except Exception:
                    pass
            sys.stdout, sys.stderr, sys.stdin = saved_streams
            self.out.flush()
            self.err.flush()
            sys.argv = saved_argv
            sys.path[:] = saved_path
            warnings.filters[:] = saved_filters
            try:
                os.chdir(saved_cwd)
            except OSError:
                pass
            if dict(os.environ) != saved_env:
                _replace_environ(saved_env)
            self._forget_our_modules()

        resident, committed = _memory_mb()
        out = self.out_sink.take().decode("utf-8", "replace")
        err = self.err_sink.take()
        if len(err) > _STDERR_KEEP:
            err = err[-_STDERR_KEEP:]
        return {
            "id": req.get("id"),
            "exit_code": exit_code,
            "stdout": "" if warm else out,
            "stderr": err.decode("utf-8", "replace"),
            "ms": round((time.perf_counter() - t0) * 1000, 3),
            "rss_mb": round(resident, 1),
            "commit_mb": round(committed, 1),
        }

    def serve(self):
        self.write_frame({"ready": True, "pid": os.getpid()})
        while True:
            req = self.read_frame()
            if req is None:
                return 0  # host closed our stdin: stop
            self.write_frame(self.run(req))


def main():
    tree = os.path.dirname(os.path.abspath(__file__))
    for i, a in enumerate(sys.argv):
        if a == "--tree" and i + 1 < len(sys.argv):
            tree = sys.argv[i + 1]
    return Worker(tree).serve()


if __name__ == "__main__":
    sys.exit(main())
