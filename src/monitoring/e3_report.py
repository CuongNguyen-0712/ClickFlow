"""Export E3 measured tables and standard Matplotlib figures."""
import csv
from pathlib import Path

from src.monitoring.latency import aggregate_trials


def write_results(output, rows, *, complete=True):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    output = Path(output)
    summary = aggregate_trials(rows, require_complete=complete)
    flat = [{key: value for key, value in row.items() if key != 'evidence'} for row in rows]
    for name, records in [('trials.csv', flat), ('summary.csv', summary)]:
        fields = sorted({key for row in records for key in row})
        # Derived exports may be rebuilt after interruption; raw trial JSON stays immutable.
        with (output / name).open('w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fields)
            writer.writeheader()
            writer.writerows(records)
    title = 'E3: fixed real-CSV cohort' if complete else 'PILOT: incomplete E3 matrix'
    colors = {100: '#1976a3', 1000: '#d56a21'}
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.6), constrained_layout=True)
    triggers = sorted({r['trigger_seconds'] for r in summary})
    for axis, metric, label in zip(axes, ['p50_latency_ms', 'p95_latency_ms'], ['p50', 'p95']):
        for offset, speed in enumerate(sorted({r['speed'] for r in summary})):
            cells = [r for r in summary if r['speed'] == speed]
            x = [triggers.index(r['trigger_seconds']) + (offset - 0.5) * 0.36 for r in cells]
            axis.bar(x, [r[metric + '_mean'] / 1000 for r in cells], width=0.34,
                     yerr=[r[metric + '_sd'] / 1000 for r in cells], capsize=4,
                     color=colors[speed], label=f'{speed}x')
            for center, cell in zip(x, cells):
                observed = [r[metric] / 1000 for r in rows if r['speed'] == speed
                            and r['trigger_seconds'] == cell['trigger_seconds']]
                axis.scatter([center] * len(observed), observed, color='#20252b', s=12, zorder=5)
        axis.set_xticks(range(len(triggers)), [str(t) for t in triggers])
        axis.set_xlabel('Configured Spark trigger (seconds)')
        axis.set_ylabel(f'Mean per-run {label} latency (seconds)')
        axis.grid(axis='y', alpha=0.2)
        axis.set_axisbelow(True)
        axis.legend(title='Replay speed')
    fig.suptitle(title + '\nDots: individual trials; error bars: sample SD')
    for suffix in ('png', 'svg'):
        fig.savefig(output / ('latency.' + suffix), dpi=170)
    plt.close(fig)
    fig, axis = plt.subplots(figsize=(9, 5.2), constrained_layout=True)
    markers = {5: 'o', 10: 's', 30: '^'}
    for cell in summary:
        scale = 100000 / cell['rows_per_trial']
        axis.errorbar(cell['small_files_created_mean'] * scale, cell['p95_latency_ms_mean'] / 1000,
            xerr=cell['small_files_created_sd'] * scale, yerr=cell['p95_latency_ms_sd'] / 1000,
            fmt=markers[cell['trigger_seconds']], color=colors[cell['speed']], capsize=4,
            markersize=8, label=f"{cell['trigger_seconds']}s / {cell['speed']}x")
    axis.set_xlabel('Small data files generated per 100,000 measured rows (<16 MiB)')
    axis.set_ylabel('Mean per-run p95 latency (seconds)')
    axis.set_title(title + '\nLatency / small-file tradeoff; error bars: sample SD')
    axis.grid(alpha=0.2)
    axis.legend(title='Trigger / replay', loc='best')
    for suffix in ('png', 'svg'):
        fig.savefig(output / ('tradeoff.' + suffix), dpi=170)
    plt.close(fig)
    with (output / 'README.md').open('w', encoding='utf-8') as stream:
        stream.write('# ' + title + '\n\n')
        stream.write('Measured differences use Spark batch-start `ingested_at`, not Delta commit time. '
                     'Signed negative differences are retained and counted. Warmup is excluded.\n\n')
        stream.write('| Trigger s | Replay | n | p50 seconds mean ± SD | p95 seconds mean ± SD | Files mean ± SD | Pace misses |\n')
        stream.write('|---:|---:|---:|---:|---:|---:|---:|\n')
        for r in summary:
            stream.write(f"| {r['trigger_seconds']} | {r['speed']}x | {r['repetitions']} | "
                f"{r['p50_latency_ms_mean']/1000:.3f} ± {r['p50_latency_ms_sd']/1000:.3f} | "
                f"{r['p95_latency_ms_mean']/1000:.3f} ± {r['p95_latency_ms_sd']/1000:.3f} | "
                f"{r['files_created_mean']:.2f} ± {r['files_created_sd']:.2f} | {r['pace_target_misses']} |\n")
        stream.write('\nThese are means of per-run percentiles, not pooled event percentiles. '
                     'Replay durations differ because every trial uses the same fixed source cohort.\n\n'
                     '![Latency](latency.png)\n\n![Tradeoff](tradeoff.png)\n')
    return summary
