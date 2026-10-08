#!/usr/bin/env python3
"""Verify retained benchmark outputs and summarize comparable cells."""
import argparse
from collections import defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summarize(root):
    completion = json.loads((root / 'completion.json').read_text())
    assert completion['complete'], 'Cohort incomplete; do not publish a complete comparison'
    design = json.loads((root / 'design.json').read_text())
    records = [json.loads(line) for line in (root / 'cells.jsonl').read_text().splitlines()]
    expected = {(f, s, c, r) for f, s, c in design['cells'] for r in range(1, design['roundsPerCell'] + 1)}
    observed = {(row['factor'], row['splits'], row['clients'], row['round']) for row in records}
    assert observed == expected and len(records) == len(expected), 'Missing or duplicate cells'
    groups = defaultdict(list)
    total = 0
    for row in records:
        assert row['allCorrect'] and len(row['results']) == design['queriesPerRound']
        counts = defaultdict(int)
        for result in row['results']:
            assert result['valid'] and not result['error'] and result['seconds'] is not None
            assert hashlib.sha256((root / (result['label'] + '.csv')).read_bytes()).hexdigest() == result['outputSha256']
            counts[result['query']] += 1
            total += 1
        assert set(counts) == set(design['workload']) and len(set(counts.values())) == 1
        groups[(row['factor'], row['splits'], row['clients'])].append(row)
    assert total == completion['measuredQueries']
    summary = []
    for (factor, splits, clients), rounds in sorted(groups.items()):
        results = [result for row in rounds for result in row['results']]
        by_query = {}
        for number in design['workload']:
            samples = [result for result in results if result['query'] == number]
            seconds = [result['seconds'] for result in samples]
            by_query[str(number)] = {'samples': len(seconds), 'medianSeconds': statistics.median(seconds),
                'p95Seconds': percentile(seconds, .95), 'observedSourceDrivers': sorted({r['sourceDrivers'] for r in samples})}
        summary.append({'factor': factor, 'splits': splits, 'clients': clients, 'measuredQueries': len(results),
            'medianQueriesPerSecond': statistics.median(row['queriesPerSecond'] for row in rounds),
            'medianRedisShardCpuCores': statistics.median(row['redisShardCpuCores'] for row in rounds),
            'medianRedisContainerCpuCores': statistics.median(row['cpu']['qpf-redis-software']['averageCpuCores'] for row in rounds),
            'medianTrinoCpuCores': statistics.median(row['cpu']['qpf-trino']['averageCpuCores'] for row in rounds),
            'queries': by_query})
    return {'complete': True, 'allCorrect': True, 'measuredQueries': total, 'cells': summary,
        'tailCaution': 'Each query/cell has a small number of samples; p95 is descriptive only.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    args = parser.parse_args()
    result = summarize(args.results)
    (args.results / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
