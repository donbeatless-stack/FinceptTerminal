// gateway/include/fmdg/pg.h
#pragma once
// PostgreSQL through libpq: an owning connection, an owning result, and an error
// that says whether the connection survived. Only what the gateway needs.
// Values always travel as parameters (PQexecParams), never spliced into SQL.

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

struct pg_conn;   // PGconn
struct pg_result; // PGresult

namespace fmdg::pg {

class Error : public std::runtime_error {
  public:
    Error(const std::string& message, std::string sqlstate, bool connection_lost)
        : std::runtime_error(message), sqlstate_(std::move(sqlstate)), connection_lost_(connection_lost) {}
    /// Five-character SQLSTATE ("23505" unique violation, "08006" connection
    /// failure, ...); empty for errors raised on the client side.
    const std::string& sqlstate() const { return sqlstate_; }
    /// The connection is unusable: discard it rather than return it to a pool.
    bool connection_lost() const { return connection_lost_; }

  private:
    std::string sqlstate_;
    bool connection_lost_;
};

class Result {
  public:
    explicit Result(pg_result* r) : r_(r) {}
    ~Result();
    Result(Result&& o) noexcept : r_(o.r_) { o.r_ = nullptr; }
    Result& operator=(Result&&) = delete;
    Result(const Result&) = delete;
    Result& operator=(const Result&) = delete;

    int rows() const;
    int columns() const;
    bool is_null(int row, int col) const;
    std::string_view text(int row, int col) const;
    double number(int row, int col) const;
    std::int64_t integer(int row, int col) const;
    /// Rows an INSERT/UPDATE/DELETE touched.
    std::uint64_t affected() const;

  private:
    pg_result* r_;
};

using Params = std::vector<std::optional<std::string>>; ///< nullopt = SQL NULL

class Connection {
  public:
    /// Opens a connection from a libpq "key=value" string: host, port, dbname,
    /// user, passfile, connect_timeout, application_name, ... Throws Error.
    static std::unique_ptr<Connection> open(const std::string& conninfo);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    /// One or more statements without parameters (DDL, BEGIN/COMMIT).
    Result exec(const std::string& sql);
    /// One statement with $1, $2, ... bound to `params` as text.
    Result exec(const std::string& sql, const Params& params);
    /// Status is OK and a round trip ("SELECT 1") succeeds.
    bool healthy();
    /// The server's process id for this session (pg_backend_pid()).
    int backend_pid() const;

  private:
    explicit Connection(pg_conn* c) : c_(c) {}
    Result check(pg_result* r, const std::string& sql);
    pg_conn* c_;
};

} // namespace fmdg::pg
