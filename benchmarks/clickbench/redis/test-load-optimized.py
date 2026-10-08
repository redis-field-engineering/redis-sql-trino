#!/usr/bin/env python3
"""Check the optimized encoder against the existing loader, plus pipeline safety."""
import datetime
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from load import column_types, encode

spec = importlib.util.spec_from_file_location('optimized', Path(__file__).with_name('load-optimized.py'))
optimized = importlib.util.module_from_spec(spec)
spec.loader.exec_module(optimized)


class LoaderTests(unittest.TestCase):
    def assert_encoding(self, batch, types):
        actual = optimized.encoded_columns(batch, types, 'arrow')
        raw = {name.lower(): values for name, values in batch.to_pydict().items()}
        expected = [[encode(value, typ).encode('utf-8') for value in raw[name]] for name, typ in types]
        self.assertEqual(actual, expected)

    def test_edge_values(self):
        batch = pa.record_batch([
            pa.array([-2**63, 2**63-1, 9007199254740993], type=pa.int64()),
            pa.array(['', '東京\x00', 'emoji 😀']),
            pa.array([-1, 0, 18000], type=pa.int32()),
            pa.array([-1, 0, 1700000000], type=pa.int64()),
            pa.array([datetime.datetime(1969, 12, 31, 23, 59, 59), datetime.datetime(1970, 1, 1), datetime.datetime(2026, 1, 1)], type=pa.timestamp('s')),
        ], names=['id', 'text', 'date', 'time', 'datetime'])
        self.assert_encoding(batch, [('id', 'bigint'), ('text', 'varchar'), ('date', 'date'), ('time', 'timestamp(3)'), ('datetime', 'timestamp(3)')])

    def test_null_rejected(self):
        with self.assertRaises(ValueError):
            optimized.encoded_columns(pa.record_batch([pa.array([None], type=pa.int64())], names=['id']), [('id', 'bigint')], 'arrow')

    def test_bounded_pipelines_and_physical_keys(self):
        class Pipeline:
            def __init__(self, owner):
                self.owner, self.commands = owner, []
            def hset(self, key, mapping):
                self.commands.append((key, mapping))
            def execute(self):
                self.owner.batches.append(self.commands)
        class Client:
            def __init__(self):
                self.batches = []
            def pipeline(self, transaction):
                self_transaction = transaction
                assert self_transaction is False
                return Pipeline(self)
        batch = pa.record_batch([pa.array([1, 1, 2, 2, 3]), pa.array(['a', 'b', 'c', 'd', 'e'])], names=['id', 'text'])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sample.parquet'
            pq.write_table(pa.Table.from_batches([batch]), path)
            options = dict(encode_only=False, batch_rows=3, pipeline_rows=2, max_pipeline_bytes=1000, encoder='arrow', table='diagnostic')
            r = Client()
            with patch.object(optimized, 'column_types', return_value=[('id', 'bigint'), ('text', 'varchar')]), patch.object(optimized, 'client', return_value=r):
                # Initialize manually since fake client has no close lifecycle.
                optimized.STATE.update(source=pq.ParquetFile(path), options=options, types=[('id', 'bigint'), ('text', 'varchar')], names=[b'id', b'text'], client=r)
                result = optimized.run_group((0, 10, 4))
            self.assertEqual(result['rows'], 4)
            self.assertEqual([len(b) for b in r.batches], [2, 2])
            self.assertEqual([key for b in r.batches for key, _ in b], [b'diagnostic:10', b'diagnostic:11', b'diagnostic:12', b'diagnostic:13'])
            options['max_pipeline_bytes'] = 1
            r.batches = []
            result = optimized.run_group((0, 0, 4))
            self.assertEqual([len(b) for b in r.batches], [1, 1, 1, 1])
            self.assertEqual(result['oversized_rows'], 4)

    def test_write_failure_is_not_success(self):
        class Pipeline:
            def hset(self, key, mapping):
                pass
            def execute(self):
                raise RuntimeError('simulated Redis failure')
        class Client:
            def pipeline(self, transaction):
                return Pipeline()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sample.parquet'
            pq.write_table(pa.table({'id': [1]}), path)
            optimized.STATE.update(source=pq.ParquetFile(path),
                                   options=dict(batch_rows=1, pipeline_rows=1, max_pipeline_bytes=1000, encoder='arrow', table='diagnostic'),
                                   types=[('id', 'bigint')], names=[b'id'], client=Client())
            with self.assertRaisesRegex(RuntimeError, 'simulated Redis failure'):
                optimized.run_group((0, 0, 1))

    def test_index_failure_rejected(self):
        class Client:
            def execute_command(self, *args):
                return [b'num_docs', 1, b'indexing', 0, b'hash_indexing_failures', 1]
        with self.assertRaisesRegex(RuntimeError, 'indexing failures'):
            optimized.verify_index(Client(), 'diagnostic', 1, 1)


if __name__ == '__main__':
    unittest.main()
