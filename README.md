# About this fork

This fork of [Fincept-Corporation/FinceptTerminal](https://github.com/Fincept-Corporation/FinceptTerminal)
adds three pieces of work: streaming strategy evaluation in the terminal's algo engine,
a standalone market-data gateway, and a Python worker pool for the AI Quant Lab.

## Streaming strategy evaluation

This part reworks how strategy conditions are evaluated in the C++ algo engine: indicators are
kept as incremental state instead of being recomputed from scratch on every bar.

| | Before | After |
| --- | --- | --- |
| Backtest condition evaluation, EMA 12/26 cross, 20,000 bars | 1087 ms | 11.2 ms (97×) |
| Realtime scan sweep, 500 symbols | 3.6–5.6 ms | 0.28–0.50 ms (10–19×) |
| Live EMA(200) error in realtime scans, worst case | 4.38 % of price | 0 |
| Test executables | 4 | 8 |

Every step is checked bar by bar against the original engine (bit-identical where the
arithmetic allows), with mutation tests, characterisation tests written before the
refactor, and per-bar verdict checksums. Design, numbers and how to reproduce them:
**[docs/STREAMING_EVALUATION.md](docs/STREAMING_EVALUATION.md)**.

### Checked against a live broker feed

The realtime scanner was then run in the app against the Alpaca paper account's IEX feed
("Price Above EMA 20", 15-minute bars, six symbols), with each match's EMA(20) recomputed
independently from Yahoo data. The live run caught what simulated feeds could not:
Alpaca history requests returned only the **oldest** 500 bars of the range
(`limit=500&sort=asc`, `next_page_token` ignored), so indicators warmed on history that
ended a month earlier. Fixed in
[`ac3a2306`](https://github.com/donbeatless-stack/FinceptTerminal/commit/ac3a2306), then re-run:

| EMA(20) minus the independent value | Before the fix | After |
| --- | --- | --- |
| MSFT | −19.02 | +0.68 |
| TSLA | −12.54 | −0.90 |
| AAPL | −13.36 | +0.35 |
| SPY | match missed (price was above its EMA) | +0.42, matches |
| Warm-up history | oldest 500 bars, ending early October | 1,094–1,193 bars, up to the last finished bar |

![Realtime scan on the Alpaca paper feed after the fix](docs/images/realtime-scan-live.png)

The remaining 0.05–0.23 % is the data source: Alpaca's free feed carries IEX trades only.
Details: [Live check against a broker feed](docs/STREAMING_EVALUATION.md#live-check-against-a-broker-feed).

## Market-data gateway (`gateway/`)

A standalone C++20 service (Asio; IOCP on Windows) that fans one market-data feed out to
many TCP clients:
- **Ports:** a push port (8589) for long connections and a request port (8588) for one-shot queries.
- **Ingest:** a lock-free SPSC ring feeds a distributor thread, which sleeps when idle without losing wake-ups.
- **Slow consumers:** per-client conflating queues mean a slow consumer costs bounded memory and never delays anyone else.
- **Stats:** an HdrHistogram-style latency histogram.
- **Feed:** Alpaca's real-time stream, through a WebSocket client written for the gateway (RFC 6455 framing, TLS with certificate checks, ping-based liveness, reconnect with backoff, error handling by code), or a synthetic feed for benchmarks. Alpaca allows one stream connection per account, so the gateway holds it and fans it out.
- **Storage:** minute bars go into PostgreSQL through a connection pool written for the gateway. The pool caps connections (counting ones being opened), never holds its lock across a network round trip, health-checks idle connections and discards broken ones. A writer thread batches upserts, so the feed never waits on the database.
- **History for the terminal:** a sync keeps the stored minutes complete from Alpaca's REST API: a backfill, then a top-up each minute, paced under the rate limit, with backpressure from the database. A `HISTORY` query, answered on a worker pool through the connection pool, returns any intraday timeframe aggregated in SQL. It also says up to when the data is complete. The terminal's realtime scan warms from it, and falls back to the broker for whatever the gateway cannot vouch for.

| | |
| --- | --- |
| Pipeline latency, ingest → socket, 1 client | 7.4 µs p50 (4.2 µs with busy-poll) |
| Distributor throughput | 12.9 M deliveries/s on one thread |
| 5 slow clients among 50 | Fast clients unaffected (p99 336 µs vs 360 µs without); slow ones conflated to 11 % of ticks |
| Connection pool vs connecting per query | 0.084 ms vs 239 ms median with 8 threads; 37,500 vs 33 queries/s (local PostgreSQL 18.6, Windows) |
| Scan warm-up, 6 symbols, broker vs gateway | 0.59 s vs 0.21 s; the same bars, symbol for symbol |
| History for 504 symbols (S&P 500 + SPY), 55 days of 15m bars | 2.1 s from the gateway; the broker path needs 504 requests, at least 2.5 min on the free plan |
| Backfill, 504 symbols × 60 days of one-minute bars | 5.37 M bars in 252 s, 512 requests, no 429s |
| Tests | 138: unit, end-to-end over sockets, concurrency, fuzzing, the feed against a fake Alpaca server (dropped links, bad keys, the connection limit, dead links), the pool, bar storage and `HISTORY` against a real PostgreSQL (including a killed server session), the REST sync against a fake server; clean under AddressSanitizer; 9 deliberately planted bugs all caught |
| Live check | Alpaca's test stream over TLS 1.3: trades reached the request port, minute bars landed in PostgreSQL about 1.4 s after each minute closed |

Architecture, design trade-offs, the full benchmark matrix and its limits:
**[gateway/README.md](gateway/README.md)**.

## Python worker pool for the AI Quant Lab

The terminal ran every AI Quant Lab call as a new Python process, so each one paid for
the interpreter and its imports (qlib, PyTorch, statsmodels, scipy) before doing any
work: 2–18 s per call. The calls now run in warm worker processes:
- **A fresh process, as far as the script can tell:** the worker sets `argv`,
  `__main__`, `sys.path`, environment, working directory and stdin per call, then restores
  them and drops the scripts' own modules, so only third-party libraries stay loaded.
  No script was changed.
- **Failures stay contained:** a worker that crashes or overruns its deadline fails
  only its own call, with no retry (it may already have trained a model). When no worker
  can start, or a call waits 5 s behind busy ones, the call runs the old way.
- **Warm-up:** opening a module imports its script in a worker while the panel is
  being filled in.

| | |
| --- | --- |
| Repeat calls, 7 real Quant Lab calls | 9–30 ms in the worker vs 2.8–17 s in a fresh process (198–958×), byte-identical output |
| In the app, Statsmodels OLS | 33 ms, then 18 ms |
| Memory | a statsmodels worker commits 5.6 GB for 189 MB resident (three bundled OpenBLAS copies reserving per-thread buffers), so workers are recycled on resident memory |
| Tests | 18 cases against a real interpreter (Python 3.11, 3.12, 3.14); 9 of 11 deliberately planted bugs caught, the other 2 change nothing a script can observe |

Design, measurements and limits: **[docs/PYTHON_WORKER_POOL.md](docs/PYTHON_WORKER_POOL.md)**.

The upstream README follows unchanged.

---

> [!IMPORTANT]
> ## 🔒 Fincept Terminal **Enterprise** — the private edition is now available.
> RealTime data · Multi-agent AI research · Live broker, Backtest & Algo execution · Priority support ·
> Equity Research · Portfolio Management · Financial Workflows · Quant Research Lab · Maritime · Geopolitics
>
> | Plan | Price |
> |:--|:--:|
> | **Exclusive** | **$15** /user/mo |
> | **Exclusive+** | **$30** /user/mo |
> | **Exclusive Pro** ⭐ Most popular | **$45** /user/mo |
>
> [![Get Enterprise](https://img.shields.io/badge/Get_Enterprise_%E2%86%92-C06524?style=for-the-badge)](https://fincept.in/enterprise/signup) &nbsp; [![Compare plans](https://img.shields.io/badge/Compare_plans-1F2328?style=for-the-badge)](https://fincept.in/pricing)
>
> <sub>No annual lock-in, cancel anytime · This open-source repo stays free under AGPL-3.0</sub>


> [!TIP]
> ## Quantcept — the AI research desk that never closes.
> Markets · Backtests · Portfolios · Watchlists · Memory — 800+ primary data sources, running around the clock.
>
> | Plan | Price |
> |:--|:--:|
> | **Free** | **$0** · 350 credits, no card |
> | **Alpha** | **$3** / 30 days |
> | **Apex** | **$8** / 30 days |
>
> [![Start free](https://img.shields.io/badge/Start_free_%E2%86%92-0E7490?style=for-the-badge)](https://quantcept.io/register) &nbsp; [![See the desk](https://img.shields.io/badge/See_the_desk-1F2328?style=for-the-badge)](https://quantcept.io)
>
> <sub>30-day passes — nothing auto-renews · Students: Apex for $5 · Free to start at [quantcept.io](https://quantcept.io)</sub>


# Fincept Terminal

<div align="center">

[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-C06524)](https://github.com/Fincept-Corporation/FinceptTerminal/blob/main/LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus)](https://isocpp.org/)
[![Qt6](https://img.shields.io/badge/Qt-6-41CD52?logo=qt&logoColor=white)](https://www.qt.io/)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Hits](https://hits.sh/github.com/Fincept-Corporation/FinceptTerminal.svg?label=Visits)](https://hits.sh/github.com/Fincept-Corporation/FinceptTerminal/)

<a href="https://trendshift.io/repositories/17028" target="_blank"><img src="https://trendshift.io/api/badge/repositories/17028" alt="Fincept-Corporation%2FFinceptTerminal | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

[![GitHub Stars](https://img.shields.io/github/stars/Fincept-Corporation/FinceptTerminal?style=social)](https://github.com/Fincept-Corporation/FinceptTerminal/stargazers)
[![GitHub Forks](https://img.shields.io/github/forks/Fincept-Corporation/FinceptTerminal?style=social)](https://github.com/Fincept-Corporation/FinceptTerminal/network/members)
[![GitHub Watchers](https://img.shields.io/github/watchers/Fincept-Corporation/FinceptTerminal?style=social)](https://github.com/Fincept-Corporation/FinceptTerminal/watchers)
[![GitHub Release](https://img.shields.io/github/v/release/Fincept-Corporation/FinceptTerminal?color=brightgreen&logo=github)](https://github.com/Fincept-Corporation/FinceptTerminal/releases)
[![GitHub Issues](https://img.shields.io/github/issues/Fincept-Corporation/FinceptTerminal)](https://github.com/Fincept-Corporation/FinceptTerminal/issues)

[![X](https://img.shields.io/badge/-X-000000?style=flat-square&logo=x&logoColor=white)](https://x.com/finceptcorp) [![Twitter](https://img.shields.io/badge/-Twitter-1DA1F2?style=flat-square&logo=twitter&logoColor=white)](https://twitter.com/intent/tweet?text=Check%20out%20FinceptTerminal&url=https%3A//github.com/Fincept-Corporation/FinceptTerminal/) [![LinkedIn](https://img.shields.io/badge/-LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/sharing/share-offsite/?url=https%3A//github.com/Fincept-Corporation/FinceptTerminal/) [![Facebook](https://img.shields.io/badge/-Facebook-1877F2?style=flat-square&logo=facebook&logoColor=white)](https://www.facebook.com/sharer/sharer.php?u=https%3A//github.com/Fincept-Corporation/FinceptTerminal/) [![Reddit](https://img.shields.io/badge/-Reddit-FF4500?style=flat-square&logo=reddit&logoColor=white)](https://www.reddit.com/r/finceptTerminal/) [![WhatsApp](https://img.shields.io/badge/-WhatsApp-25D366?style=flat-square&logo=whatsapp&logoColor=white)](https://api.whatsapp.com/send?text=Check%20out%20FinceptTerminal%3A%20https%3A//github.com/Fincept-Corporation/FinceptTerminal/) [![Telegram](https://img.shields.io/badge/-Telegram-26A5E4?style=flat-square&logo=telegram&logoColor=white)](https://t.me/finceptTerminalcorp)

### **Your Thinking is the Only Limit. The Data Isn't.**

State-of-the-art financial intelligence platform with institutional-grade financial analytics, AI automation, and unlimited data connectivity.

[📥 Download](https://github.com/Fincept-Corporation/FinceptTerminal/releases) · [🏢 Enterprise](https://fincept.in/enterprise) · [💳 Pricing](https://fincept.in/pricing) · [📖 Manual](https://fincept.in/manual) · [💬 Discussions](https://github.com/Fincept-Corporation/FinceptTerminal/discussions) · [💬 Discord](https://discord.gg/ae87a8ygbN) · [📈 Quantcept](https://quantcept.io)

<sub>English · [Deutsch](docs/translations/README.de.md) · [Español](docs/translations/README.es.md) · [Français](docs/translations/README.fr.md) · [हिन्दी](docs/translations/README.hi.md) · [日本語](docs/translations/README.ja.md) · [한국어](docs/translations/README.ko.md) · [简体中文](docs/translations/README.zh-CN.md) · [繁體中文](docs/translations/README.zh-TW.md)</sub>

![Fincept Terminal](https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/FinceptBanner.png)

<table>
  <tr>
    <td align="center" width="25%"><a href="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/EquityResearch.png"><img src="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/EquityResearch.png" width="100%"/></a><br/><sub><b>Equity Research</b></sub></td>
    <td align="center" width="25%"><a href="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/Portfolio.png"><img src="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/Portfolio.png" width="100%"/></a><br/><sub><b>Portfolio</b></sub></td>
    <td align="center" width="25%"><a href="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/News.png"><img src="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/News.png" width="100%"/></a><br/><sub><b>News</b></sub></td>
    <td align="center" width="25%"><a href="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/NodeEditor.png"><img src="https://raw.githubusercontent.com/Fincept-Corporation/FinceptTerminal/main/images/NodeEditor.png" width="100%"/></a><br/><sub><b>Node Editor</b></sub></td>
  </tr>
</table>

</div>

---

## About

**Fincept Terminal** is a native C++20 desktop terminal for financial research — Qt6 UI, embedded Python 3.11 analytics, one binary, no Electron.

Two editions run on one data core. **[Enterprise](https://fincept.in/enterprise)** is the private, closed-source build the team develops daily, for funds, family offices and research desks. **This repo** is the free AGPL-3.0 edition — learning, personal use, academic research — shipping one release a month.

Use the open build if you're a student, hobbyist or academic. Use Enterprise if you're a firm, or if the terminal is how you earn: AGPL copyleft won't apply, and the open build's real cost is your own data and LLM bills, charged per token with no ceiling.

Fincept account (this terminal): Free, or Pro at $10/month for monthly cloud credits, higher API limits and priority support.

| | Open Source | **Enterprise** |
|---|---|---|
| **Licence** | AGPL-3.0 — strong copyleft | Proprietary — no copyleft to manage |
| **Cost** | Free, plus your own data and LLM bills | **$15 / $30 / $45** per user/month |
| **Data** | Free public feeds, your own API keys | Private datasets, deeper history, point-in-time |
| **AI** | Bring your own LLM key | 400–5,000 credits included · multi-agent research · private dataroom |
| **Trading** | Paper + live trading via your broker | Managed live routing, execution analytics, priority SLA |
| **Controls** | — | SSO/SAML, audit logs, RBAC, SLA-backed support |

[**See Enterprise →**](https://fincept.in/enterprise) · [Full comparison](https://fincept.in/comparison) · [Pricing](https://fincept.in/pricing) · [FAQ](https://fincept.in/faq)

---

## Enterprise

41 modules across six desks — agentic research, quant lab and backtesting, deep fundamental research, markets and execution, macro and global intelligence, and your own workspace. All of it in a [700-page manual](https://fincept.in/manual).

| | **Exclusive** | **Exclusive+** | **Exclusive Pro** ★ |
|---|---|---|---|
| | **$15**/user/mo | **$30**/user/mo | **$45**/user/mo |
| AI credits / month | 400 | 2,000 | 5,000 |
| Deep research + agent teams | — | ✓ | ✓ |
| Live broker trading + algo | — | — | ✓ |

Monthly billing, no annual lock-in, no seat minimum — **$180–$540 per user per year**, against roughly $27,000 for a Bloomberg seat. Quarterly and university terms are on the [pricing page](https://fincept.in/pricing). There is no negotiated pricing and no separate commercial licence.

Enterprise needs its own account — free Fincept logins don't sign in to it.

[**Create account**](https://fincept.in/enterprise/signup) · [**Book a walkthrough**](https://calendly.com/nikultilak/fincept-terminal-demo)

---

## Install

<!-- DOWNLOAD-TABLE-START -->
Latest release: **v4.5.1** — [View all releases](https://github.com/Fincept-Corporation/FinceptTerminal/releases/tag/v4.5.1)

| Platform | Download | Run |
|----------|----------|-----|
| **Windows x64** | [FinceptTerminal-Windows-x64-setup.exe](https://github.com/Fincept-Corporation/FinceptTerminal/releases/download/v4.5.1/FinceptTerminal-4.5.1-windows-x64-setup.exe) | Run installer → launch `FinceptTerminal.exe` |
| **Linux x64 (AppImage)** | [FinceptTerminal-Linux-x64.run](https://github.com/Fincept-Corporation/FinceptTerminal/releases/download/v4.5.1/FinceptTerminal-4.5.1-linux-x64-setup.run) | `chmod +x` → run installer |
| **Linux x64 (Debian/Ubuntu)** | [FinceptTerminal-Linux-x64.deb](https://github.com/Fincept-Corporation/FinceptTerminal/releases/download/v4.5.1/FinceptTerminal-4.5.1-linux-x64.deb) | `sudo apt install ./FinceptTerminal-*.deb` |
| **Linux x64 (Fedora/RHEL)** | [FinceptTerminal-Linux-x64.rpm](https://github.com/Fincept-Corporation/FinceptTerminal/releases/download/v4.5.1/FinceptTerminal-4.5.1-linux-x64.rpm) | `sudo dnf install ./FinceptTerminal-*.rpm` |
| **macOS Apple Silicon** | [FinceptTerminal-macOS-arm64.dmg](https://github.com/Fincept-Corporation/FinceptTerminal/releases/download/v4.5.1/FinceptTerminal-4.5.1-macos-arm64-setup.dmg) | Open DMG → drag to Applications |
<!-- DOWNLOAD-TABLE-END -->

**Build from source** — Linux/macOS: `git clone … && ./setup.sh`. Windows and manual builds, the pinned toolchain (**CMake 3.27.7 · Ninja 1.11.1 · Qt 6.8.3 · Python 3.11.9**) and troubleshooting live in **[docs/GETTING_STARTED.md](docs/GETTING_STARTED.md)**. Versions are pinned — newer or older ones are unsupported.

> Looking for the Enterprise build? It has its own signed installers for Windows, macOS and Linux, behind an Enterprise login — [get them here](https://fincept.in/enterprise).

---

## What's in the open build

- **Analytics** — DCF, portfolio optimisation, VaR/Sharpe, derivatives pricing, fixed income, alternatives, plus an 18-module QuantLib suite
- **AI** — 37 trader/investor, economic and geopolitics agents; bring your own key (OpenAI, Anthropic, Gemini, Groq, DeepSeek, OpenRouter, Ollama)
- **Data** — 100+ connectors: FRED, IMF, World Bank, DBnomics, AkShare, Polygon, Kraken, Yahoo Finance, government APIs
- **Trading** — crypto and equity feeds, paper-trading engine, 16 broker integrations
- **Automation** — visual node editor, MCP tools, AI Quant Lab (ML, factor discovery, RL)
- **Global intelligence** — maritime tracking, geopolitical analysis, relationship mapping

Native C++20 · Qt6 · embedded Python 3.11 · single binary · no Node.js, no browser runtime.

---

## How this repo is maintained

This repo **stays public and will not be deleted**. Everything already released stays released.

It now ships **one release a month** rather than continuous development, because the team's daily work is on Enterprise. Issues and pull requests are still reviewed, and fixes land on the monthly cycle. Security reports go to [support@fincept.in](mailto:support@fincept.in).

---

## Contributing

New data connectors, AI agents, analytics modules, C++ screens and documentation are all welcome.

[Contributing guide](docs/CONTRIBUTING.md) · [C++ guide](docs/CPP_CONTRIBUTOR_GUIDE.md) · [Python guide](docs/PYTHON_CONTRIBUTOR_GUIDE.md) · [Architecture](docs/ARCHITECTURE.md) · [Report a bug](https://github.com/Fincept-Corporation/FinceptTerminal/issues) · [Request a feature](https://github.com/Fincept-Corporation/FinceptTerminal/discussions)

---

## Also from Fincept

- **[Fincept Data API](https://docs.fincept.in)** — 500+ REST endpoints, 423,000+ instruments, 2,000+ sources. Free tier included with any account.
- **[Quantcept](https://quantcept.io)** — AI research desk for markets: primary data, backtests, portfolios, documents and 24/7 monitors. Free to start; Alpha $14 / Apex $24 per 30 days.

---

## License

**AGPL-3.0-or-later** — full text in [LICENSE](LICENSE).

Free for personal use, learning and academic research. AGPL-3.0 is **strong copyleft, not permissive**: distribute a modified build, or run one as a service other people reach, and you must publish your changes under the same licence. For most legal teams that is the row that ends the discussion — which is why firms take **[Enterprise](https://fincept.in/enterprise)**, which is proprietary and carries no copyleft obligations to manage. Personal, non-distributed use carries no obligations.

Fincept no longer sells a separate commercial or academic licence for this repository. Commercial, enterprise and university needs are served by **[Fincept Terminal Enterprise](https://fincept.in/enterprise)** at the published prices above.

**Trademarks.** "Fincept", "Fincept Terminal" and the Fincept logo are trademarks of Fincept Corporation. Use in any forked, derivative, rebranded or commercial product requires prior written permission.

Questions: [support@fincept.in](mailto:support@fincept.in) · [Terms](https://fincept.in/terms) · [Privacy](https://fincept.in/privacy)

© 2025–2026 Fincept Corporation. All rights reserved.

---

<div align="center">

### **Your Thinking is the Only Limit. The Data Isn't.**

<div align="center">
<a href="https://star-history.dera.page/#Fincept-Corporation/FinceptTerminal&Date">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://star-history.dera.page/svg?repos=Fincept-Corporation/FinceptTerminal&type=Date&theme=dark" />
   <source media="(prefers-color-scheme: light)" srcset="https://star-history.dera.page/svg?repos=Fincept-Corporation/FinceptTerminal&type=Date" />
   <img alt="Star History Chart" src="https://star-history.dera.page/svg?repos=Fincept-Corporation/FinceptTerminal&type=Date" />
 </picture>
</a>
</div>

[![Repobeats](https://repobeats.axiom.co/api/embed/fincept-corporation-finceptterminal.svg "Repobeats analytics image")](https://repobeats.axiom.co)

[![Email](https://img.shields.io/badge/Email-support@fincept.in-blue)](mailto:support@fincept.in)

⭐ **Star** · 🔄 **Share** · 🤝 **Contribute**

</div>
