"""Checks for the API-mode gate and the credential-safe, read-only diagnostic."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error


spec = importlib.util.spec_from_file_location("cloud_api_status", Path(__file__).with_name("cloud-api-status.py"))
status = importlib.util.module_from_spec(spec)
spec.loader.exec_module(status)


class TestCloudApiStatus(unittest.TestCase):
    def test_requires_active_database_and_explicit_matching_boolean(self):
        for mode, flag in (("default", False), ("oss", True)):
            self.assertTrue(status.mode_matches({"status": "active", "supportOSSClusterApi": flag}, mode))
            self.assertFalse(status.mode_matches({"status": "active", "supportOSSClusterApi": not flag}, mode))
            for invalid in (None, 0, 1, "false", "true"):
                self.assertFalse(status.mode_matches({"status": "active", "supportOSSClusterApi": invalid}, mode))
            self.assertFalse(status.mode_matches({"status": "active"}, mode))
            self.assertFalse(status.mode_matches({"status": "pending", "supportOSSClusterApi": flag}, mode))

    def test_only_get_requests_and_no_credentials_in_output(self):
        requests = []

        def reply(request, **kwargs):
            requests.append(request)
            self.assertEqual(request.get_method(), "GET")
            self.assertIsNone(request.data)
            if "/tasks/" in request.full_url:
                data = {"taskId": "abc", "status": "processing-error", "response": {
                    "password": "database-password", "error": {"type": "PROVISION_FAILURE",
                        "description": "account-token user-token", "password": "database-password"}}}
            else:
                data = {"status": "active", "supportOSSClusterApi": True,
                        "publicEndpoint": "private-host", "password": "database-password"}
            return io.BytesIO(json.dumps(data).encode())

        output = io.StringIO()
        argv = ["status", "1", "2", "--client-mode", "default", "--task",
                "705f141b-1ed5-43d4-b15a-7ce51e039234"]
        with patch.dict(status.os.environ, {"REDIS_CLOUD_API_KEY": "account-token",
                "REDIS_CLOUD_API_SECRET_KEY": "user-token"}), patch.object(status.sys, "argv", argv), \
                patch.object(status.urllib.request, "urlopen", reply), contextlib.redirect_stdout(output):
            self.assertEqual(status.main(), 1)
        self.assertEqual(len(requests), 2)
        for value in ("account-token", "user-token", "database-password", "private-host"):
            self.assertNotIn(value, output.getvalue())
        self.assertFalse(json.loads(output.getvalue())["serverClientModeMatches"])

    def test_api_error_does_not_echo_response_body(self):
        output = io.StringIO()
        error = urllib.error.HTTPError("https://api.redislabs.com/v1/example", 403, "Forbidden", {},
                io.BytesIO(b"private response"))
        with patch.dict(status.os.environ, {"REDIS_CLOUD_API_KEY": "account-token",
                "REDIS_CLOUD_API_SECRET_KEY": "user-token"}), \
                patch.object(status.sys, "argv", ["status", "1", "2", "--client-mode", "oss"]), \
                patch.object(status.urllib.request, "urlopen", side_effect=error), contextlib.redirect_stderr(output):
            self.assertEqual(status.main(), 2)
        self.assertIn("HTTP 403", output.getvalue())
        self.assertNotIn("private response", output.getvalue())


if __name__ == "__main__":
    unittest.main()
