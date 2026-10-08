#!/usr/bin/env python3
"""Validate complete scan coverage and summarize fixed and automatic split sweeps."""
import json
import statistics
from pathlib import Path

root = Path(__file__).parent
reference = json.loads((root / 'sample.json').read_text())
metrics = ('elapsed_ms', 'jvm_cpu_cores', 'redis_cpu_cores', 'point_scan_median_ms',
           'point_scan_p95_ms', 'redis.request-wall-time', 'redis.row-conversion-time',
           'redis.cursor.requests', 'input_bytes', 'trino_cpu_ms', 'redis.exact-hash-reads',
           'redis.exact-hash-read-wait-time', 'redis.exact-hash-read-batches')


def summarize(samples, configurations):
    summary = {}
    for splits in configurations:
        summary[str(splits)] = {}
        for query in ('scan', 'q5', 'q6', 'q3'):
            matching = [s for s in samples if s['splits'] == splits and s['query'] == query]
            assert len(matching) == 4, (splits, query, len(matching))
            for sample in matching:
                assert sample['result'] == reference[query]
                expected = 1 if query == 'q3' else (8 if splits == 0 else splits)
                assert sample['redis.aggregate.requests'] == expected
                assert sample['source_drivers'] == expected
                assert sample['redis.rows.received'] == (1 if query == 'q3' else reference['rows'])
                if query != 'q3' and expected > 1:
                    assert sample['source_worker_count'] >= 2
            measured = [s for s in matching if s['attempt'] != 0]
            values = {m: statistics.median(s[m] for s in measured) for m in metrics}
            values['max_point_p95_ms'] = max(s['point_scan_p95_ms'] for s in measured)
            values['range_elapsed_ms'] = [min(s['elapsed_ms'] for s in measured), max(s['elapsed_ms'] for s in measured)]
            summary[str(splits)][query] = values
    return summary


fixed = summarize(json.loads((root / 'results.json').read_text())['samples'], (1, 2, 4, 8))
datasets = [('summary.json', fixed)]
if (root / 'auto-results.json').exists():
    datasets.append(('auto-summary.json', summarize(json.loads((root / 'auto-results.json').read_text())['samples'], (0,))))
for filename, summary in datasets:
    for splits, queries in summary.items():
        for query, values in queries.items():
            values['speedup'] = fixed['1'][query]['elapsed_ms'] / values['elapsed_ms']
            print(splits, query, round(values['elapsed_ms']/1000, 3), round(values['speedup'], 2),
                  'point p95 ms', round(values['point_scan_p95_ms'], 2))
    (root / filename).write_text(json.dumps(summary, indent=2)+'\n')
