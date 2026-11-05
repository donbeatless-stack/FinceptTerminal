#pragma once
// Product identity for the Keystone Terminal build: name, wordmark, logo mark,
// links of this build, and the switch for upstream's in-app promotions.
//
// Chrome (window title, header, login, About, tray) reads the name from here;
// longer translated sentences carry "Keystone Terminal" literally. Internal
// identifiers (QSettings organisation and application names, the AppData
// folder, FINCEPT_* environment variables, the executable file name, file
// extensions, bundle ids) deliberately keep their original values: changing
// them would orphan users' existing settings and data.
#include <QColor>
#include <QPainter>
#include <QPainterPath>
#include <QPixmap>
#include <QString>

namespace fincept::brand {

inline constexpr const char* kProductName = "Keystone Terminal";
inline constexpr const char* kWordmarkPrimary = "KEYSTONE";
inline constexpr const char* kWordmarkSecondary = "TERMINAL";
inline constexpr const char* kIncubatorLine = "Incubated at Penn Venture Lab";

/// Links of this build (AGPL-3.0: users can get its source) and of upstream.
inline constexpr const char* kSourceUrl = "https://github.com/donbeatless-stack/FinceptTerminal";
inline constexpr const char* kIssuesUrl = "https://github.com/donbeatless-stack/FinceptTerminal/issues";
inline constexpr const char* kNewIssueUrl = "https://github.com/donbeatless-stack/FinceptTerminal/issues/new";
inline constexpr const char* kDocsUrl = "https://github.com/donbeatless-stack/FinceptTerminal/tree/main/docs";
inline constexpr const char* kLicenseUrl = "https://github.com/donbeatless-stack/FinceptTerminal/blob/main/LICENSE";
inline constexpr const char* kUpstreamUrl = "https://github.com/Fincept-Corporation/FinceptTerminal";

/// Contact address shown on About / Contact. Empty: those screens point to
/// the issue tracker instead of listing an e-mail address.
inline constexpr const char* kContactEmail = "";

inline bool has_contact_email() noexcept {
    return kContactEmail[0] != '\0';
}

/// Upstream's in-app promotion of its own paid edition (toolbar button,
/// dashboard strip, reminder dialog, mentions). Off in this build.
inline constexpr bool kShowUpstreamPromos = false;

/// Runtime accessor for kShowUpstreamPromos, so call sites read as ordinary
/// conditions (no "conditional expression is constant" warnings).
inline bool show_upstream_promos() noexcept {
    return kShowUpstreamPromos;
}

/// Wordmark typeface: Bahnschrift ships with Windows 10+; Qt falls back to the
/// next family elsewhere.
inline constexpr const char* kWordmarkFamily = "'Bahnschrift','Segoe UI','Helvetica Neue',sans-serif";

inline QString product_name() {
    return QString::fromLatin1(kProductName);
}

/// The keystone mark: a semicircular arch whose crown stone (the keystone)
/// stands proud of the ring. Drawn on a 24-unit grid, centred in `r`.
/// Same geometry as tools/brand/make_icons.py.
struct LogoPaths {
    QPainterPath keystone;
    QPainterPath ring;
};

inline LogoPaths logo_paths(const QRectF& r) {
    const qreal s = qMin(r.width(), r.height()) / 24.0;
    const QPointF o(r.center().x() - 12.0 * s, r.center().y() - 12.0 * s);
    auto p = [&](qreal x, qreal y) { return QPointF(o.x() + x * s, o.y() + y * s); };
    const QPointF c = p(12, 18);

    QPainterPath outer;
    outer.addEllipse(c, 10 * s, 10 * s);
    QPainterPath inner;
    inner.addEllipse(c, 6 * s, 6 * s);
    QPainterPath upper;
    upper.addRect(QRectF(p(0, 0), p(24, 18)));
    QPainterPath gap; // clears a slot either side of the keystone
    gap.moveTo(c);
    gap.lineTo(p(8.69, 6.47));
    gap.lineTo(p(15.31, 6.47));
    gap.closeSubpath();

    QPainterPath keystone;
    keystone.moveTo(p(9.56, 6.5));
    keystone.lineTo(p(14.44, 6.5));
    keystone.lineTo(p(13.25, 12.13));
    keystone.lineTo(p(10.75, 12.13));
    keystone.closeSubpath();

    return {keystone, outer.subtracted(inner).intersected(upper).subtracted(gap)};
}

/// Logo mark on a transparent `px`×`px` pixmap (device pixels = px × dpr, so
/// it stays crisp on high-DPI screens). The keystone takes `accent`; the
/// arch ring takes `ring` (default: the accent at reduced opacity).
inline QPixmap logo_pixmap(int px, const QColor& accent, qreal dpr = 1.0, QColor ring = QColor()) {
    if (!ring.isValid()) {
        ring = accent;
        ring.setAlpha(120);
    }
    QPixmap pm(qRound(px * dpr), qRound(px * dpr));
    pm.setDevicePixelRatio(dpr);
    pm.fill(Qt::transparent);
    QPainter painter(&pm);
    painter.setRenderHint(QPainter::Antialiasing);
    painter.setPen(Qt::NoPen);
    const LogoPaths paths = logo_paths(QRectF(0, 0, px, px));
    painter.setBrush(ring);
    painter.drawPath(paths.ring);
    painter.setBrush(accent);
    painter.drawPath(paths.keystone);
    return pm;
}

} // namespace fincept::brand
