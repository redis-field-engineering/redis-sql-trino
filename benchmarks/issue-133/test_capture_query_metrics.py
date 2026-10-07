import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("capture", pathlib.Path(__file__).with_name("capture-query-metrics.py"))
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)


class TestCapture(unittest.TestCase):
    def test_keeps_operator_metrics_without_double_counting_stage_copies(self):
        metrics = {"redis.exact-hash-reads": {"total": 1000}}
        info = {
            "queryId": "query", "state": "FAILED", "errorCode": {"name": "EXCEEDED_TIME_LIMIT"},
            "queryStats": {"physicalInputPositions": 1000, "operatorSummaries": [
                {"operatorType": "ScanFilterAndProjectOperator", "connectorMetrics": metrics},
                {"operatorType": "AggregationOperator", "connectorMetrics": {}},
            ]},
            "outputStage": {"connectorMetrics": metrics},
        }
        result = capture.capture(info)
        self.assertEqual(len(result["scanMetrics"]), 1)
        self.assertEqual(result["scanMetrics"][0]["connectorMetrics"], metrics)
        self.assertEqual(result["error"]["name"], "EXCEEDED_TIME_LIMIT")

    def test_missing_metrics_are_not_reported_as_zero(self):
        result = capture.capture({"queryId": "query", "state": "RUNNING"})
        self.assertEqual(result["scanMetrics"], [])
        self.assertIsNone(result["queryStats"]["physicalInputPositions"])

    def test_sql_selection_excludes_explain(self):
        self.assertEqual(capture.normalized(capture.Q5), capture.normalized("select count(distinct UserID) from hits"))
        self.assertNotEqual(capture.normalized(capture.Q5), capture.normalized("EXPLAIN " + capture.Q5))


if __name__ == "__main__":
    unittest.main()
