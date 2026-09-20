#!/usr/bin/env python3
"""Render one row per validated run. Never pool percentiles or hide failures."""
import json
from pathlib import Path
import sys


def render(root):
    rows = []
    for file in sorted(root.glob('*-*/result.json')):
        result = json.loads(file.read_text())
        if result['status'] != 'validated':
            continue
        rows.append((file.parent.name, result))
    contexts = {}
    for name, result in rows:
        placement = json.loads((root/name/'placement.json').read_text())
        mount = placement['mount']['filesystems'][0]
        cfg = result['config']
        context = (mount['fstype'], mount['target'], cfg['warmup_secs'], cfg['duration_secs'])
        contexts.setdefault(context, []).append(name)
    lines = ['# Queue benchmark results', '']
    for (filesystem, mount, warmup, duration), names in contexts.items():
        label = 'All runs' if len(contexts) == 1 else ', '.join(names)
        lines.append(f'**{label}: {filesystem} at `{mount}`; {warmup}s warmup + {duration}s measurement**, followed by drain.')
        if filesystem == 'tmpfs':
            lines.append('Storage is RAM-backed: sync calls do not measure persistent-device flush latency.')
        lines.append('')
    lines += [
        'One row per run; no pooled percentiles. Rates count the measurement cohort through its final confirmation and delivery, including drain. Latencies exclude warmup by intended send time. Confirm and delivery columns start at admission; intended→delivery also includes pacing and credit waiting. See README for the full method and limits.', '',
        '| Run | Contract | Bytes | Offered/s | Completed/s | Confirm p99 ms | Delivery p99 ms | Intended→delivery p99 ms |',
        '|---|---|---:|---:|---:|---:|---:|---:|']
    for name, r in rows:
        cfg = r['config']
        lines.append(f"| {name} | {r['contract']['sync_policy']} | {cfg['payload_bytes']} | {cfg['rate'] or 'saturation'} | {r['cohort_completed_per_sec']:,.0f} | {r['confirm']['from_admission']['p99_ms']:.3f} | {r['delivery']['from_admission']['p99_ms']:.3f} | {r['delivery']['from_schedule']['p99_ms']:.3f} |")
    lines += ['', 'Container memory includes charged file cache and kernel memory; anonymous memory is shown separately. All peaks are sampled. Broker CPU covers client setup through final settlement. Client RSS includes SDKs and benchmark bookkeeping.', '',
              '| Run | Broker peak total MiB | Broker peak anonymous MiB | Broker CPU s | Client peak RSS MiB | Client CPU s |',
              '|---|---:|---:|---:|---:|---:|']
    tick_hz = json.loads((root/'provenance.json').read_text())['clock_ticks_per_second']
    for name, r in rows:
        resource = r['broker_resources']
        clients = [x['client'] for x in r['timeline']]
        rss = [x['rss_kib'] for x in clients if x['rss_kib'] is not None]
        ticks = [x['cpu_ticks'] for x in clients if x['cpu_ticks'] is not None]
        mib = lambda key: f"{resource[key]/(1024*1024):.1f}" if resource.get('available') else 'unavailable'
        cpu = f"{resource['cpu_seconds']:.2f}" if resource.get('available') else 'unavailable'
        client_rss = f'{max(rss)/1024:.1f}' if rss else 'unavailable'
        client_cpu = f'{(ticks[-1]-ticks[0])/tick_hz:.2f}' if ticks else 'unavailable'
        lines.append(f"| {name} | {mib('sampled_peak_memory_current_bytes')} | {mib('sampled_peak_anon_bytes')} | {cpu} | {client_rss} | {client_cpu} |")
    failures = sorted(root.glob('*-*/failure.json'))
    if failures:
        lines += ['', '## Failed runs', '']
        for f in failures:
            lines.append(f"- {f.parent.name}: {json.loads(f.read_text())['error']}")
    (root/'TABLE.md').write_text('\n'.join(lines)+'\n')
    return len(rows)

if __name__ == '__main__':
    render(Path(sys.argv[1]))
