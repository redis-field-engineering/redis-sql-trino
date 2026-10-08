import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("probe", pathlib.Path(__file__).with_name("query-engine-probe.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class Client:
    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.commands = reply, error, []

    def execute_command(self, *command):
        self.commands.append(command)
        if self.error:
            raise self.error
        return self.reply


class TestProbe(unittest.TestCase):
    def test_health_requires_complete_expected_count_under_both_protocols(self):
        replies = [[1, ["documents", "10000000"]],
                   {"results": [{"extra_attributes": {"documents": "10000000"}}], "warning": []}]
        for reply in replies:
            client = Client(reply=reply)
            result = probe.probe(client, ["FT.AGGREGATE", "hits", "*"], expected_count=10000000)
            self.assertTrue(result["complete"])
            self.assertIsNone(result["error"])
            self.assertEqual(result["documents"], 10000000)
            self.assertEqual(len(client.commands), 1)
            self.assertIsNone(result["successfulSeconds"])

    def test_partial_warning_wrong_count_and_malformed_health_fail_without_retry(self):
        for reply in [[1, ["documents", "9999999"]],
                      {"results": [{"extra_attributes": {"documents": "10000000"}}],
                       "warning": ["Timeout limit was reached"]},
                      {"results": []}, [1, ["other", "10000000"]],
                      [1, ["documents", 10000000.5]]]:
            client = Client(reply=reply)
            result = probe.probe(client, ["FT.AGGREGATE", "hits", "*"], expected_count=10000000)
            self.assertFalse(result["complete"])
            self.assertIsNotNone(result["error"])
            self.assertEqual(len(client.commands), 1)
            self.assertIsNone(result["successfulSeconds"])

    def test_retains_parser_and_topology_failures_without_retries_or_success_timings(self):
        for message in ("SEARCH_PARSE_ARGS Bad arguments for PARAMS", "shards topology update is either transient or has failed"):
            client = Client(error=RuntimeError(message))
            result = probe.probe(client, ["FT.AGGREGATE", "hits", "*"])
            self.assertEqual(len(client.commands), 1)
            self.assertEqual(result["error"]["message"], message)
            self.assertIsNone(result["successfulSeconds"])
            self.assertFalse(result["complete"])

    def test_cleans_up_partial_cursor_on_same_connection(self):
        client = Client(reply=[[1, ["id", "9007199254740993"]], 42])
        result = probe.probe(client, ["FT.AGGREGATE", "hits", "*", "WITHCURSOR"])
        self.assertEqual(client.commands[-1], ("FT.CURSOR", "DEL", "hits", 42))
        self.assertTrue(result["cursorDeleted"])
        self.assertFalse(result["complete"])
        self.assertIsNone(result["successfulSeconds"])

    def test_rejects_non_query_commands_and_retains_expression_boundaries(self):
        with self.assertRaises(ValueError):
            probe.validate(["FLUSHALL", "hits", "*"])
        command = ["FT.AGGREGATE", "hits", "*", "FILTER", 'exists(@url) && @url == "a b"']
        client = Client(reply=[0])
        self.assertTrue(probe.probe(client, command)["complete"])
        self.assertEqual(list(client.commands[0]), command)


if __name__ == "__main__":
    unittest.main()
