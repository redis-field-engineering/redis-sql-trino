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
