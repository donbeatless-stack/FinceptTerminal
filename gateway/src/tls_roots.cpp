// gateway/src/tls_roots.cpp
// Its own translation unit: <wincrypt.h> #defines X509_NAME and friends, which
// collide with OpenSSL's types unless wincrypt comes first and OpenSSL's headers
// then #undef them — an include order easiest to guarantee in a file this small.
#include "tls_roots.h"

#ifdef _WIN32
#include <windows.h>
#include <wincrypt.h>
#endif

#include <openssl/ssl.h>
#include <openssl/x509.h>

namespace fmdg {

int load_system_roots(ssl_ctx_st* ctx) {
    SSL_CTX_set_default_verify_paths(ctx);
    int added = 0;
#ifdef _WIN32
    HCERTSTORE store = CertOpenSystemStoreW(0, L"ROOT");
    if (store == nullptr)
        return 0;
    X509_STORE* x509_store = SSL_CTX_get_cert_store(ctx);
    PCCERT_CONTEXT cert = nullptr;
    while ((cert = CertEnumCertificatesInStore(store, cert)) != nullptr) {
        const unsigned char* der = cert->pbCertEncoded;
        if (X509* x = d2i_X509(nullptr, &der, static_cast<long>(cert->cbCertEncoded))) {
            if (X509_STORE_add_cert(x509_store, x) == 1)
                ++added; // a duplicate fails harmlessly
            X509_free(x);
        }
    }
    CertCloseStore(store, 0);
#endif
    return added;
}

} // namespace fmdg
