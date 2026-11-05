"""Render the Keystone Terminal app icons and README artwork.

The mark is the same one drawn in src/ui/theme/Brand.h (24-unit grid): a
semicircular arch whose crown stone, the keystone, stands proud of the ring.

Usage:  python tools/brand/make_icons.py
Writes (file names kept so CMake and packaging need no change):
  fincept-qt/resources/fincept.ico                       Windows exe icon
  fincept-qt/resources/fincept.icns                      macOS bundle icon
  fincept-qt/resources/in.fincept.FinceptTerminal.png    Linux desktop icon, 256 px
  fincept_icon.ico                                       repo-root copy
  docs/images/keystone-logo.png                          README logo, 512 px
  docs/images/keystone-banner.png                        README banner
"""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
SS = 1024  # master size; everything is drawn here and downscaled

BG = (11, 17, 24, 255)        # #0b1118  base
SURFACE = (17, 26, 36, 255)   # #111a24  surface
EDGE = (36, 52, 68, 255)      # #243444  hairline
TEAL = (45, 212, 191, 255)    # #2dd4bf  accent
TEXT = (230, 237, 245, 255)   # #e6edf5
MUTED = (107, 124, 143, 255)  # #6b7c8f


RING = (45, 212, 191, 120)    # accent at reduced opacity


def draw_mark(img, box, keystone_rgba, ring_rgba):
    """Arch ring plus keystone, alpha-composited onto `img` in square `box`."""
    x0, y0, size = box
    s = size / 24.0

    def p(x, y):
        return (x0 + x * s, y0 + y * s)

    w, h = img.size
    ring = Image.new("L", (w, h), 0)
    d = ImageDraw.Draw(ring)
    cx, cy = p(12, 18)
    d.ellipse([cx - 10 * s, cy - 10 * s, cx + 10 * s, cy + 10 * s], fill=255)
    d.ellipse([cx - 6 * s, cy - 6 * s, cx + 6 * s, cy + 6 * s], fill=0)
    d.rectangle([0, cy, w, h], fill=0)
    d.polygon([(cx, cy), p(8.69, 6.47), p(15.31, 6.47)], fill=0)

    key = Image.new("L", (w, h), 0)
    ImageDraw.Draw(key).polygon([p(9.56, 6.5), p(14.44, 6.5), p(13.25, 12.13), p(10.75, 12.13)], fill=255)

    for mask, rgba in ((ring, ring_rgba), (key, keystone_rgba)):
        layer = Image.new("RGBA", (w, h), rgba[:3] + (0,))
        alpha = mask.point(lambda v, a=rgba[3]: v * a // 255)
        layer.putalpha(alpha)
        img.alpha_composite(layer)


def app_icon_master():
    img = Image.new("RGBA", (SS, SS), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    pad = SS * 0.04
    d.rounded_rectangle([pad, pad, SS - pad, SS - pad], radius=SS * 0.2, fill=SURFACE, outline=EDGE,
                        width=int(SS * 0.012))
    inner = SS * 0.74
    draw_mark(img, ((SS - inner) / 2, (SS - inner) / 2, inner), TEAL, RING)
    return img


def logo_master():
    """Mark alone on a transparent background."""
    img = Image.new("RGBA", (SS, SS), (0, 0, 0, 0))
    draw_mark(img, (0, 0, SS), TEAL, RING)
    return img


def banner():
    w, h = 2400, 600
    scale = 2
    img = Image.new("RGBA", (w * scale, h * scale), BG)
    d = ImageDraw.Draw(img)
    mark = 300 * scale
    mx, my = 260 * scale, (h * scale - mark) // 2
    d.rectangle([0, h * scale - 6 * scale, w * scale, h * scale], fill=TEAL)
    draw_mark(img, (mx, my, mark), TEAL, RING)
    fonts = Path("C:/Windows/Fonts")
    bahn = ImageFont.truetype(str(fonts / "bahnschrift.ttf"), 150 * scale)
    try:
        bahn.set_variation_by_name("Bold")
    except Exception:
        pass
    light = ImageFont.truetype(str(fonts / "bahnschrift.ttf"), 150 * scale)
    try:
        light.set_variation_by_name("Light")
    except Exception:
        pass
    tag = ImageFont.truetype(str(fonts / "segoeui.ttf"), 46 * scale)
    tx = mx + mark + 90 * scale
    d.text((tx, 150 * scale), "KEYSTONE", font=bahn, fill=TEXT)
    kw = d.textlength("KEYSTONE ", font=bahn)
    d.text((tx + kw, 150 * scale), "TERMINAL", font=light, fill=MUTED)
    d.text((tx + 6 * scale, 345 * scale), "Research, analytics and trading in one native desktop terminal",
           font=tag, fill=MUTED)
    return img.resize((w, h), Image.LANCZOS)


def main():
    icon = app_icon_master()
    res = ROOT / "fincept-qt" / "resources"
    sizes = [(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    icon.save(res / "fincept.ico", sizes=sizes)
    icon.save(ROOT / "fincept_icon.ico", sizes=sizes)
    icon.save(res / "fincept.icns")
    icon.resize((256, 256), Image.LANCZOS).save(res / "in.fincept.FinceptTerminal.png")

    images = ROOT / "docs" / "images"
    images.mkdir(parents=True, exist_ok=True)
    logo_master().resize((512, 512), Image.LANCZOS).save(images / "keystone-logo.png")
    banner().convert("RGB").save(images / "keystone-banner.png", optimize=True)
    print("icons and artwork written")


if __name__ == "__main__":
    main()
