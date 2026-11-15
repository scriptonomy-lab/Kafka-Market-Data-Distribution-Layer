"""
Draw the evidence charts for one experiment run, ONLY from files inside that
run's folder (lag.csv, storage_batches.csv, events.json, summary.json).
Re-running this on the same folder always gives the same pictures; nothing
is fetched or typed.

    lag.png        consumer lag of all four groups on ONE shared axis, with the
                   phases shaded. The brief in one picture: Storage's line
                   climbs and falls; the others stay flat.
    isolation.png  the same data as small multiples, each group on its own
                   scale, so you can see Pricing/Risk/Monitor really did not
                   move (their lag is tiny next to Storage's on a shared axis).
    catchup.png    Storage's event age per batch (how stale the data it was
                   writing was), log scale: high right after restart, back to
                   live once caught up.
    lag_by_topic.png  Storage's lag split by topic (FX, crypto, US stocks).

Experiment F (scale-out) runs get, instead of lag/isolation/catchup:
    ownership.png  one row per topic-partition, coloured by the instance that
                   owned it in each sampler tick (blank = nobody), with every
                   docker action marked: the rebalances, drawn from data.
    lag_by_topic.png
    throughput.png messages written per Storage instance per 5 s.

Time zero is the start of the BASELINE phase (F: the steady step). The setup phase (Storage joining
and catching up on older data) is not part of the experiment and is left out.

Run:  make charts ARGS=results/<run-id>
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # render to files; no display needed
import matplotlib.pyplot as plt  # noqa: E402  (must follow matplotlib.use)

# Colour follows the ENTITY, never its rank: each group always has the same
# colour in every chart. Values are the first four slots of a categorical
# palette validated for colour-vision deficiency (light mode).
GROUP_COLORS = {
    "storage-svc": "#2a78d6",  # blue  - the group under test
    "pricing-svc": "#eb6834",  # orange
    "risk-svc": "#1baf7a",  # aqua
    "monitor-svc": "#eda100",  # yellow
}
INK, INK_2, GRID, SURFACE, BAND = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb", "#ebeae5"
PHASE_LABELS = {
    "baseline": "Baseline",
    "outage": "Outage",
    "catchup": "Catch-up",
    "settle": "Settle",
}
# Instances and topics are entities too: colour by first appearance / fixed order
SERIES_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
TOPIC_LABEL = {
    "marketdata.fx.quotes.v1": "FX",
    "marketdata.crypto.quotes.v1": "Crypto",
    "marketdata.equities.us.quotes.v1": "US stocks",
}


def _style(ax) -> None:
    """Recessive axes: light grid, no top/right spines, secondary ink."""
    ax.set_facecolor(SURFACE)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=9)


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _phase_spans(events: dict, t0: datetime, t_end: float) -> list[tuple[str, float, float]]:
    """(name, start_s, end_s) for each phase from baseline to settle."""
    order = [p for p in ("baseline", "outage", "catchup", "settle", "verify") if p in events]
    spans = []
    for i, name in enumerate(order):
        if name == "verify":
            break
        start = (_parse(events[name]) - t0).total_seconds()
        end = (_parse(events[order[i + 1]]) - t0).total_seconds() if i + 1 < len(order) else t_end
        spans.append((name, start, end))
    return spans


def _shade(ax, spans, label: bool) -> None:
    """Shade the outage (and, lighter, the catch-up); mark the restart."""
    for name, start, end in spans:
        if name == "outage":
            ax.axvspan(start, end, color=BAND, zorder=0, linewidth=0)
        elif name == "catchup":
            ax.axvspan(start, end, color=BAND, alpha=0.5, zorder=0, linewidth=0)
            ax.axvline(start, color=INK_2, linewidth=0.8, linestyle=(0, (3, 3)), zorder=1)
        if label:
            ax.text(
                (start + end) / 2,
                1.01,
                PHASE_LABELS.get(name, name),
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="bottom",
                fontsize=9,
                color=INK_2,
            )


def _end_labels(ax, series: dict) -> None:
    """Direct labels at line ends. Lines that finish within 4% of the y-range
    of each other share ONE label, so text never overprints."""
    ends = sorted(((ys[-1], xs[-1], g) for g, (xs, ys) in series.items() if xs), reverse=True)
    y_span = max((max(ys) for xs, ys in series.values() if ys), default=1) or 1
    clusters: list[list] = []
    for y, x, g in ends:
        if clusters and abs(clusters[-1][-1][0] - y) <= 0.04 * y_span:
            clusters[-1].append((y, x, g))
        else:
            clusters.append([(y, x, g)])
    for cl in clusters:
        y, x = cl[0][0], max(c[1] for c in cl)
        names = ", ".join(c[2] for c in cl)
        ax.annotate(
            names, (x, y), xytext=(6, 3), textcoords="offset points", fontsize=8.5, color=INK
        )


def make_charts(run_dir: Path) -> list[Path]:
    run_dir = Path(run_dir)
    events = json.loads((run_dir / "events.json").read_text())
    if "scale_out" in events:
        return make_scale_charts(run_dir, events)
    with open(run_dir / "lag.csv") as f:
        # v2 lag.csv has one row per group x topic; "*" is the group total
        rows = [
            r for r in csv.DictReader(f) if r["phase"] != "setup" and r.get("topic", "*") == "*"
        ]
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    t0 = _parse(events.get("baseline", rows[0]["ts"]))
    t_end = (_parse(rows[-1]["ts"]) - t0).total_seconds()
    spans = _phase_spans(events, t0, t_end)

    series: dict[str, tuple[list, list]] = {g: ([], []) for g in GROUP_COLORS}
    for r in rows:
        if r["group"] in series and r["lag"] not in ("", "None"):
            series[r["group"]][0].append((_parse(r["ts"]) - t0).total_seconds())
            series[r["group"]][1].append(int(float(r["lag"])))

    subtitle = f"{summary.get('run_id', run_dir.name)}"
    if summary.get("simulated_feed"):
        subtitle += "  ·  SIMULATED FEED - NOT EVIDENCE"
    written = []

    # ---- lag.png: one shared axis ---------------------------------------------
    fig, ax = plt.subplots(figsize=(10, 5), dpi=160, facecolor=SURFACE)
    _style(ax)
    _shade(ax, spans, label=True)
    for g, (xs, ys) in series.items():
        if xs:
            ax.plot(xs, ys, color=GROUP_COLORS[g], linewidth=2, label=g, solid_capstyle="round")
    _end_labels(ax, series)
    ax.set_xlabel("seconds since baseline start", color=INK_2, fontsize=9)
    ax.set_ylabel("consumer lag (messages)", color=INK_2, fontsize=9)
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, fontsize=8.5, loc="upper left", labelcolor=INK)
    fig.suptitle(
        "Only Storage falls behind; the other consumers keep up",
        x=0.01,
        ha="left",
        fontsize=12,
        fontweight="bold",
        color=INK,
    )
    ax.set_title(subtitle, loc="left", fontsize=8.5, color=INK_2, pad=18)
    fig.text(
        0.01,
        0.01,
        "Storage commits offsets every few seconds and the others every second, so lag rises "
        "between commits (sawtooth). That is commit cadence, not falling behind.",
        fontsize=8,
        color=INK_2,
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    path = run_dir / "lag.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    written.append(path)

    # ---- isolation.png: small multiples, own scale each -------------------------
    fig, axes = plt.subplots(
        len(GROUP_COLORS), 1, figsize=(10, 7.5), dpi=160, sharex=True, facecolor=SURFACE
    )
    for ax, (g, (xs, ys)) in zip(axes, series.items(), strict=True):
        _style(ax)
        _shade(ax, spans, label=(ax is axes[0]))
        if xs:
            ax.plot(xs, ys, color=GROUP_COLORS[g], linewidth=2)
        ax.set_ylim(bottom=0)
        ax.set_ylabel(g, color=INK, fontsize=9, rotation=0, ha="right", va="center")
    axes[-1].set_xlabel("seconds since baseline start", color=INK_2, fontsize=9)
    fig.suptitle(
        "Consumer lag per group, each on its own scale (messages)",
        x=0.01,
        ha="left",
        fontsize=12,
        fontweight="bold",
        color=INK,
    )
    fig.tight_layout()
    path = run_dir / "isolation.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    written.append(path)

    # ---- catchup.png: Storage event age per batch --------------------------------
    batches_path = run_dir / "storage_batches.csv"
    if batches_path.exists():
        with open(batches_path) as f:
            batches = list(csv.DictReader(f))
        pts = [
            ((_parse(b["written_at"]) - t0).total_seconds(), max(1, int(b["max_age_ms"])))
            for b in batches
        ]
        pts = [(x, y) for x, y in pts if x >= 0]  # drop setup-phase batches
        if pts:
            fig, ax = plt.subplots(figsize=(10, 4.2), dpi=160, facecolor=SURFACE)
            _style(ax)
            _shade(ax, spans, label=True)
            ax.scatter(
                [p[0] for p in pts],
                [p[1] for p in pts],
                s=12,
                color=GROUP_COLORS["storage-svc"],
                edgecolors=SURFACE,
                linewidths=0.6,
            )
            ax.set_yscale("log")
            ax.set_xlabel("seconds since baseline start", color=INK_2, fontsize=9)
            ax.set_ylabel("age of oldest message in batch (ms, log)", color=INK_2, fontsize=9)
            fig.suptitle(
                "Storage works through its backlog, then returns to live data",
                x=0.01,
                ha="left",
                fontsize=12,
                fontweight="bold",
                color=INK,
            )
            ax.set_title(subtitle, loc="left", fontsize=8.5, color=INK_2, pad=18)
            fig.tight_layout()
            path = run_dir / "catchup.png"
            fig.savefig(path, facecolor=SURFACE)
            plt.close(fig)
            written.append(path)
    written.append(lag_by_topic_chart(run_dir, t0, subtitle, spans=spans))
    return written


def _action_lines(ax, actions: list[dict], t0: datetime, label: bool) -> None:
    """Dashed vertical line at every docker action, labelled at the top."""
    for a in actions:
        x = (_parse(a["started_at"]) - t0).total_seconds()
        ax.axvline(x, color=INK_2, linewidth=0.8, linestyle=(0, (3, 3)), zorder=1)
        if label:
            ax.text(
                x,
                1.01,
                f"{a['action']} {a['target'].replace('tradermade-kafka-distribution-', '')}",
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="bottom",
                fontsize=7.5,
                color=INK_2,
            )


def lag_by_topic_chart(
    run_dir: Path, t0: datetime, subtitle: str, spans=None, actions=None
) -> Path:
    """storage-svc lag per topic: which asset class Storage fell behind on."""
    series: dict[str, tuple[list, list]] = {}
    with open(run_dir / "lag.csv") as f:
        for r in csv.DictReader(f):
            if r["group"] != "storage-svc" or r.get("topic", "*") == "*" or r["phase"] == "setup":
                continue
            if r["lag"] in ("", "None"):
                continue
            xs, ys = series.setdefault(r["topic"], ([], []))
            xs.append((_parse(r["ts"]) - t0).total_seconds())
            ys.append(int(float(r["lag"])))
    fig, ax = plt.subplots(figsize=(10, 4.5), dpi=160, facecolor=SURFACE)
    _style(ax)
    if spans:
        _shade(ax, spans, label=True)
    if actions:
        _action_lines(ax, actions, t0, label=True)
    for i, (topic, (xs, ys)) in enumerate(sorted(series.items())):
        ax.plot(
            xs, ys, color=SERIES_COLORS[i % 4], linewidth=1.8, label=TOPIC_LABEL.get(topic, topic)
        )
    ax.set_ylim(bottom=0)
    ax.set_xlabel("seconds since start", color=INK_2, fontsize=9)
    ax.set_ylabel("storage-svc lag (messages)", color=INK_2, fontsize=9)
    ax.legend(frameon=False, fontsize=8.5, loc="upper left", labelcolor=INK)
    fig.suptitle(
        "Storage lag per topic", x=0.01, ha="left", fontsize=12, fontweight="bold", color=INK
    )
    ax.set_title(subtitle, loc="left", fontsize=8.5, color=INK_2, pad=18)
    fig.tight_layout()
    path = run_dir / "lag_by_topic.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return path


def make_scale_charts(run_dir: Path, events: dict) -> list[Path]:
    """Experiment F: ownership timeline, lag per topic, throughput per instance."""
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    actions = json.loads((run_dir / "actions.json").read_text())
    actions = [a for a in actions if _parse(a["started_at"]) >= _parse(events["steady"])]
    t0 = _parse(events["steady"])
    subtitle = summary.get("run_id", run_dir.name)
    if summary.get("simulated_feed"):
        subtitle += "  ·  SIMULATED FEED - NOT EVIDENCE"
    written = []

    # ---- ownership.png ----------------------------------------------------------
    owners: dict[tuple, list[tuple[float, str]]] = {}
    with open(run_dir / "ownership.csv") as f:
        for r in csv.DictReader(f):
            if r["phase"] == "setup":
                continue
            tp = (r["topic"], int(r["partition"]))
            owners.setdefault(tp, []).append(
                ((_parse(r["ts"]) - t0).total_seconds(), r["instance"])
            )
    instances = []
    for samples in owners.values():
        for _x, inst in samples:
            if inst and inst not in instances:
                instances.append(inst)
    color = {inst: SERIES_COLORS[i % 4] for i, inst in enumerate(sorted(instances))}
    # Partitions that carry data = those partition_activity.csv analysed
    with_data = set()
    try:
        with open(run_dir / "partition_activity.csv") as f:
            with_data = {(r["topic"], int(r["partition"])) for r in csv.DictReader(f)}
    except FileNotFoundError:
        pass
    order = sorted(owners, key=lambda tp: (tp[0], tp[1]))
    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=160, facecolor=SURFACE)
    _style(ax)
    ax.grid(False)
    # After its container was stopped or killed, an instance can still be
    # listed as owner (a SIGKILLed member stays in the group until
    # session.timeout.ms). Draw that ownership hatched and faded: the
    # partition has an owner on paper and nobody processing it.
    dead_after = {}
    for step, inst in summary.get("victims", {}).items():
        act = summary.get("steps", {}).get(step, {})
        if act.get("started_at"):
            dead_after[inst] = (_parse(act["started_at"]) - t0).total_seconds()

    def bar(row, x0, x1, inst):
        dead_from = dead_after.get(inst)
        if dead_from is not None and x1 > dead_from:
            if x0 < dead_from:
                bar(row, x0, dead_from, inst)
                x0 = dead_from
            ax.broken_barh(
                [(x0, x1 - x0)],
                (row - 0.4, 0.8),
                facecolors=SURFACE,
                edgecolors=color[inst],
                hatch="////",
                linewidth=0.6,
            )
            return
        ax.broken_barh([(x0, x1 - x0)], (row - 0.4, 0.8), facecolors=color[inst], linewidth=0)

    for row, tp in enumerate(order):
        samples = owners[tp]
        for (x0, inst), (x1, _next) in zip(
            samples, samples[1:] + [(samples[-1][0] + 0.5, "")], strict=True
        ):
            if inst:
                bar(row, x0, x1, inst)
    ax.set_yticks(range(len(order)))
    ax.set_yticklabels(
        [f"{TOPIC_LABEL.get(t, t)} {p}{' *' if (t, p) in with_data else ''}" for t, p in order],
        fontsize=7.5,
    )
    ax.invert_yaxis()
    _action_lines(ax, actions, t0, label=True)
    handles = [plt.Rectangle((0, 0), 1, 1, color=color[i]) for i in sorted(instances)]
    ax.legend(
        handles,
        sorted(instances),
        title="instance",
        frameon=False,
        fontsize=8,
        title_fontsize=8,
        loc="upper left",
        bbox_to_anchor=(1.0, 1.0),
    )
    ax.set_xlabel(
        "seconds since steady step start  ·  * = partition carries data  ·  blank = no owner"
        "  ·  hatched = owner already stopped/killed",
        color=INK_2,
        fontsize=9,
    )
    fig.suptitle(
        "Who owned each topic-partition, through scale-out, clean stop and crash",
        x=0.01,
        ha="left",
        fontsize=12,
        fontweight="bold",
        color=INK,
    )
    ax.set_title(subtitle, loc="left", fontsize=8.5, color=INK_2, pad=18)
    fig.tight_layout()
    path = run_dir / "ownership.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    written.append(path)

    # ---- lag_by_topic.png -------------------------------------------------------
    written.append(lag_by_topic_chart(run_dir, t0, subtitle, actions=actions))

    # ---- throughput.png ---------------------------------------------------------
    series: dict[str, tuple[list, list]] = {}
    with open(run_dir / "throughput.csv") as f:
        for r in csv.DictReader(f):
            xs, ys = series.setdefault(r["instance"], ([], []))
            xs.append(float(r["t_s"]))
            ys.append(float(r["msgs_per_s"]))
    fig, ax = plt.subplots(figsize=(10, 4.5), dpi=160, facecolor=SURFACE)
    _style(ax)
    _action_lines(ax, actions, t0, label=True)
    for inst, (xs, ys) in sorted(series.items()):
        ax.plot(
            xs, ys, color=color.get(inst, INK_2), linewidth=1.8, label=inst, drawstyle="steps-post"
        )
    ax.set_ylim(bottom=0)
    ax.set_xlabel("seconds since steady step start (5 s buckets)", color=INK_2, fontsize=9)
    ax.set_ylabel("messages written per second", color=INK_2, fontsize=9)
    ax.legend(title="instance", frameon=False, fontsize=8, title_fontsize=8, loc="upper left")
    fig.suptitle(
        "Storage throughput per instance",
        x=0.01,
        ha="left",
        fontsize=12,
        fontweight="bold",
        color=INK,
    )
    ax.set_title(subtitle, loc="left", fontsize=8.5, color=INK_2, pad=18)
    fig.tight_layout()
    path = run_dir / "throughput.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    written.append(path)
    return written


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python -m experiments.make_charts results/<run-id>", file=sys.stderr)
        return 2
    for p in make_charts(Path(sys.argv[1])):
        print(f"wrote {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
