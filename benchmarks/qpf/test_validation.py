"""Guard against publishing rounded, partial, or incomplete benchmark results."""
import json
from pathlib import Path
import tempfile
import unittest

from run import validate
from summarize import summarize


class AcceptanceTest(unittest.TestCase):
    def test_adjacent_large_integers_remain_distinct(self):
        reference = {'rows': [[9_007_199_254_740_993]]}
        self.assertTrue(validate([[9_007_199_254_740_993]], reference))
        self.assertFalse(validate([[9_007_199_254_740_992]], reference))

    def test_partial_response_is_not_a_success(self):
        reference = {'rows': [[1, 2, 3]]}
        self.assertFalse(validate([], reference))
        self.assertFalse(validate([[1, 2]], reference))
        self.assertFalse(validate([[1, 2, 3], [1, 2, 3]], reference))

    def test_incomplete_matrix_cannot_be_summarized_as_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'completion.json').write_text(json.dumps({'complete': True}))
            (root / 'design.json').write_text(json.dumps({'cells': [[0, 1, 1], [2, 1, 1]], 'roundsPerCell': 1}))
            (root / 'cells.jsonl').write_text(json.dumps({'factor': 0, 'splits': 1, 'clients': 1, 'round': 1}) + '\n')
            with self.assertRaisesRegex(AssertionError, 'Missing or duplicate'):
                summarize(root)


if __name__ == '__main__':
    unittest.main()
