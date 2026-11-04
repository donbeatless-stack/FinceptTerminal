// gateway/src/tls_roots.h
#pragma once

struct ssl_ctx_st; // SSL_CTX

namespace fmdg {

/// Makes `ctx` trust the operating system's root certificates: OpenSSL's
/// default paths (the distro bundle on Linux) plus, on Windows, every
/// certificate in the system ROOT store, which OpenSSL does not read by itself.
/// Returns how many Windows roots were added (0 elsewhere).
int load_system_roots(ssl_ctx_st* ctx);

} // namespace fmdg
