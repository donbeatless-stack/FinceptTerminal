// gateway/src/pg.cpp
#include "fmdg/pg.h"

#include <libpq-fe.h>

#include <charconv>

namespace fmdg::pg {

namespace {

std::string trimmed(const char* s) {
    std::string out = s ? s : "";
    while (!out.empty() && (out.back() == '\n' || out.back() == ' '))
        out.pop_back();
    return out;
}

// SQLSTATE class 08 is "connection exception"; 57P01-57P03 mean the server is
// shutting down or not accepting connections. Any of them, or a connection
// whose status went bad, leaves nothing worth keeping.
bool lost(PGconn* c, const std::string& sqlstate) {
    return PQstatus(c) != CONNECTION_OK || sqlstate.rfind("08", 0) == 0 || sqlstate == "57P01" ||
           sqlstate == "57P02" || sqlstate == "57P03";
}

} // namespace

// ── Result ───────────────────────────────────────────────────────────────────

Result::~Result() {
    if (r_)
        PQclear(r_);
}
int Result::rows() const {
    return PQntuples(r_);
}
int Result::columns() const {
    return PQnfields(r_);
}
bool Result::is_null(int row, int col) const {
    return PQgetisnull(r_, row, col) != 0;
}
std::string_view Result::text(int row, int col) const {
    return {PQgetvalue(r_, row, col), static_cast<std::size_t>(PQgetlength(r_, row, col))};
}
double Result::number(int row, int col) const {
    const std::string_view t = text(row, col);
    double v = 0;
    if (std::from_chars(t.data(), t.data() + t.size(), v).ec != std::errc{})
        throw Error("not a number: " + std::string(t), {}, false);
    return v;
}
std::int64_t Result::integer(int row, int col) const {
    const std::string_view t = text(row, col);
    std::int64_t v = 0;
    if (std::from_chars(t.data(), t.data() + t.size(), v).ec != std::errc{})
        throw Error("not an integer: " + std::string(t), {}, false);
    return v;
}
std::uint64_t Result::affected() const {
    const char* n = PQcmdTuples(r_);
    std::uint64_t v = 0;
    if (n && *n)
        std::from_chars(n, n + std::char_traits<char>::length(n), v);
    return v;
}

// ── Connection ───────────────────────────────────────────────────────────────

std::unique_ptr<Connection> Connection::open(const std::string& conninfo) {
    PGconn* c = PQconnectdb(conninfo.c_str());
    if (c == nullptr)
        throw Error("libpq: out of memory", {}, true);
    if (PQstatus(c) != CONNECTION_OK) {
        const std::string why = trimmed(PQerrorMessage(c));
        PQfinish(c);
        throw Error("cannot connect to PostgreSQL: " + why, {}, true);
    }
    return std::unique_ptr<Connection>(new Connection(c));
}

Connection::~Connection() {
    PQfinish(c_);
}

Result Connection::check(pg_result* r, const std::string& sql) {
    const ExecStatusType st = r ? PQresultStatus(r) : PGRES_FATAL_ERROR;
    if (st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK)
        return Result(r);
    std::string sqlstate, message;
    if (r) {
        const char* code = PQresultErrorField(r, PG_DIAG_SQLSTATE);
        sqlstate = code ? code : "";
        message = trimmed(PQresultErrorMessage(r));
        PQclear(r);
    }
    if (message.empty())
        message = trimmed(PQerrorMessage(c_));
    const std::string head = sql.substr(0, 60) + (sql.size() > 60 ? "..." : "");
    throw Error(message + " [" + (sqlstate.empty() ? "no sqlstate" : sqlstate) + "] in: " + head, sqlstate,
                lost(c_, sqlstate));
}

Result Connection::exec(const std::string& sql) {
    return check(PQexec(c_, sql.c_str()), sql);
}

Result Connection::exec(const std::string& sql, const Params& params) {
    std::vector<const char*> values;
    values.reserve(params.size());
    for (const auto& p : params)
        values.push_back(p ? p->c_str() : nullptr);
    return check(PQexecParams(c_, sql.c_str(), static_cast<int>(values.size()), nullptr, values.data(), nullptr,
                              nullptr, 0),
                 sql);
}

bool Connection::healthy() {
    if (PQstatus(c_) != CONNECTION_OK)
        return false;
    try {
        return exec("SELECT 1").rows() == 1;
    } catch (const Error&) {
        return false;
    }
}

int Connection::backend_pid() const {
    return PQbackendPID(c_);
}

} // namespace fmdg::pg
