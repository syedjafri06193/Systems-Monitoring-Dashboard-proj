# Systems Monitoring Dashboard

A real-time infrastructure dashboard with sub-second end-to-end latency:
push-based metric collection, a compact binary wire format over WebSocket,
a tiered time-series store, robust-statistics anomaly detection, and
hysteresis-based alerting.

[![CI](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/actions/workflows/ci.yml/badge.svg)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/actions/workflows/ci.yml)

## Highlights

- **P99 under 400 ms** from collection to pixel, with the latency budget computed and asserted in tests
- **Binary wire codec** shared by Go and TypeScript, kept in lockstep by generated fixtures and fuzzing
- **Hand-written RFC 6455 WebSocket server** with frame-size limits, mask enforcement and same-origin checks
- **Anomaly detection** using robust statistics, with alert hysteresis to avoid flapping
- **Cardinality limits** so one bad label can't exhaust memory

## Quick start

Requires Go 1.24+ and Node 22+.

```bash
cd v1
go test -race ./...
go run ./cmd/gateway -loadgen -series 1000 -hz 5

# in another terminal
cd v1/web && npm ci && npm run dev    # open http://localhost:5173
```

More detail in [`v1/README.md`](v1/README.md).

## Repository layout

```
.
├── 1—Documentation/   all documentation (start with its README index)
└── v1/
    ├── cmd/           gateway, agent, detector, loadgen, genfixtures
    ├── internal/      wire, ws, hub, ring, store, detect, alert, latency, cardinality
    └── web/           TypeScript + uPlot front end (Vite, Vitest)
```

## Documentation

All documentation lives in [`1—Documentation/`](1—Documentation/README.md).

## Versions and feedback

| Version | Summary | Feedback |
|---|---|---|
| [v1](v1/) | Push collection, binary WebSocket stream, tiered store, anomaly detection and alerting | — |

Add a row per version as new iterations land.

<!-- project-metrics:start -->
## Project Metrics

[![Stars](https://img.shields.io/github/stars/syedjafri06193/Systems-Monitoring-Dashboard-proj?style=flat-square)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/stargazers) [![Forks](https://img.shields.io/github/forks/syedjafri06193/Systems-Monitoring-Dashboard-proj?style=flat-square)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/forks) [![Open issues](https://img.shields.io/github/issues/syedjafri06193/Systems-Monitoring-Dashboard-proj?style=flat-square)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/issues) [![Last commit](https://img.shields.io/github/last-commit/syedjafri06193/Systems-Monitoring-Dashboard-proj/improvements?style=flat-square)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/commits/improvements) [![Commits](https://img.shields.io/github/commit-activity/t/syedjafri06193/Systems-Monitoring-Dashboard-proj?style=flat-square)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/commits/improvements) [![Top language](https://img.shields.io/github/languages/top/syedjafri06193/Systems-Monitoring-Dashboard-proj?style=flat-square)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj) [![Repo size](https://img.shields.io/github/repo-size/syedjafri06193/Systems-Monitoring-Dashboard-proj?style=flat-square)](https://github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj)

### Code

| Metric | Value |
|---|---|
| Lines of code | 7,566 |
| Source files | 51 |
| Tracked files | 66 |
| Documentation | 8 Markdown files (16,269 words), 1 PDF |
| Commits | 9 |
| Contributors | 1 |
| Active | 2026-09-11 → 2026-10-28 |

| Language | Lines | Share |
|---|---:|---:|
| Go | 6,322 | 83.6% |
| TypeScript | 1,192 | 15.8% |
| HTML | 35 | 0.5% |
| Docker | 10 | 0.1% |
| YAML | 5 | 0.1% |
| Modula-2 | 2 | 0.0% |

<sub>Code stats as of 2026-10-28, counted with pygount (comments and blank lines excluded). Badges update live.</sub>
<!-- project-metrics:end -->
