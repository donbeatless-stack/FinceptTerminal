// gateway/tools/pg_bench.cpp — what the connection pool buys: the same query,
// from N threads, once with a new connection per query and once through
// Pool<pg::Connection>. Each query is a parameterised round trip; the connect
// path pays TCP + the startup packet + SCRAM authentication (and, on Windows, a
// new server process per connection) every time.
//
//   pg_bench --conninfo "host=localhost dbname=fmdg user=fmdg passfile=..." \
//            [--threads 8] [--queries 2000] [--connect-queries 100] [--pool-size 8]
#include "fmdg/latency_histogram.h"
#include "fmdg/pg.h"
#include "fmdg/pool.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Run {
    double seconds = 0;
    std::uint64_t queries = 0;
    std::atomic<std::uint64_t> errors{0};
    fmdg::LatencyHistogram latency; // per query, including any connect/acquire
};

void run_threads(int threads, int per_thread, Run& run, const std::function<void(int)>& one_query) {
    std::atomic<bool> go{false};
    std::vector<std::thread> pool;
    for (int t = 0; t < threads; ++t)
        pool.emplace_back([&, t] {
            while (!go.load())
                std::this_thread::yield();
            for (int i = 0; i < per_thread; ++i) {
                const auto t0 = Clock::now();
                try {
                    one_query(t * per_thread + i);
                } catch (const std::exception& e) {
                    if (run.errors++ == 0)
                        std::fprintf(stderr, "first error: %s\n", e.what());
                    continue;
                }
                run.latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
            }
        });
    const auto start = Clock::now();
    go = true;
    for (auto& th : pool)
        th.join();
    run.seconds = std::chrono::duration<double>(Clock::now() - start).count();
    run.queries = run.latency.count();
}

void report(const char* name, const Run& r) {
    auto ms = [](std::int64_t ns) { return static_cast<double>(ns) / 1e6; };
    std::printf("%-22s %8llu queries %8.2f s %10.0f q/s   p50 %7.3f ms  p99 %7.3f ms  max %8.3f ms  errors %llu\n",
                name, static_cast<unsigned long long>(r.queries), r.seconds,
                static_cast<double>(r.queries) / r.seconds, ms(r.latency.percentile(0.50)),
                ms(r.latency.percentile(0.99)), ms(r.latency.max()), static_cast<unsigned long long>(r.errors.load()));
}

} // namespace

int main(int argc, char** argv) {
    std::string conninfo;
    int threads = 8, queries = 2000, connect_queries = 100;
    std::size_t pool_size = 8;
    for (int i = 1; i < argc; ++i) {
        const std::string_view a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "pg_bench: %s needs a value\n", argv[i]);
                std::exit(2);
            }
            return argv[++i];
        };
        if (a == "--conninfo")
            conninfo = next();
        else if (a == "--threads")
            threads = std::stoi(next());
        else if (a == "--queries")
            queries = std::stoi(next());
        else if (a == "--connect-queries")
            connect_queries = std::stoi(next());
        else if (a == "--pool-size")
            pool_size = std::stoul(next());
        else {
            std::fprintf(stderr, "usage: pg_bench --conninfo STR [--threads N] [--queries N] "
                                 "[--connect-queries N] [--pool-size N]\n");
            return 2;
        }
    }
    if (conninfo.empty()) {
        std::fprintf(stderr, "pg_bench: --conninfo is required\n");
        return 2;
    }

    const std::string sql = "SELECT $1::int + 1";
    std::printf("%d threads; query: %s\n", threads, sql.c_str());

    Run connect_each;
    run_threads(threads, connect_queries, connect_each, [&](int i) {
        auto conn = fmdg::pg::Connection::open(conninfo);
        conn->exec(sql, {std::to_string(i)});
    });
    report("connect per query", connect_each);

    fmdg::PoolConfig cfg;
    cfg.max_size = pool_size;
    fmdg::Pool<fmdg::pg::Connection> pool(
        cfg, [&] { return fmdg::pg::Connection::open(conninfo); },
        [](fmdg::pg::Connection& c) { return c.healthy(); });
    Run pooled;
    run_threads(threads, queries, pooled, [&](int i) {
        auto lease = pool.acquire();
        lease->exec(sql, {std::to_string(i)});
    });
    char name[32];
    std::snprintf(name, sizeof name, "pool (size %zu)", pool_size);
    report(name, pooled);

    const auto st = pool.stats();
    std::printf("pool: %llu connections opened for %llu queries; max wait in acquire %.3f ms\n",
                static_cast<unsigned long long>(st.created), static_cast<unsigned long long>(st.acquired),
                static_cast<double>(st.wait_ns_max) / 1e6);
    std::printf("throughput ratio: %.1fx; median latency ratio: %.1fx\n",
                (static_cast<double>(pooled.queries) / pooled.seconds) /
                    (static_cast<double>(connect_each.queries) / connect_each.seconds),
                static_cast<double>(connect_each.latency.percentile(0.5)) /
                    static_cast<double>(pooled.latency.percentile(0.5)));
    return 0;
}
