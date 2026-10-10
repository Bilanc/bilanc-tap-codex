import unittest
from unittest import mock

import requests
import requests_mock

import tap_codex
from tap_codex import RetriableServerError, authed_get

URL = "https://api.chatgpt.com/v1/analytics/codex/workspaces/ws/usage"


@mock.patch("backoff._sync.time.sleep", lambda _seconds: None)
@mock.patch("tap_codex.get_request_timeout", lambda: 1)
class AuthedGetRetryTest(unittest.TestCase):
    def test_retries_5xx_then_succeeds(self):
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(
                URL,
                [
                    {"status_code": 500, "text": "boom"},
                    {"status_code": 503, "text": "boom"},
                    {"status_code": 200, "json": {"data": [], "has_more": False}},
                ],
            )
            resp = authed_get("daily_usage", URL, {"start_time": 1})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(m.call_count, 3)

    def test_gives_up_after_max_attempts(self):
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(URL, status_code=500, text="boom")
            with self.assertRaises(RetriableServerError) as ctx:
                authed_get("daily_usage", URL, {"start_time": 1})

        self.assertEqual(m.call_count, tap_codex.MAX_REQUEST_ATTEMPTS)
        self.assertEqual(ctx.exception.response.status_code, 500)
        self.assertIn("500 Server Error", str(ctx.exception))

    def test_retries_429(self):
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(
                URL,
                [
                    {"status_code": 429, "text": "slow down"},
                    {"status_code": 200, "json": {"data": []}},
                ],
            )
            resp = authed_get("daily_usage", URL, {})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(m.call_count, 2)

    def test_retries_connection_error(self):
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(
                URL,
                [
                    {"exc": requests.exceptions.ConnectionError},
                    {"status_code": 200, "json": {"data": []}},
                ],
            )
            resp = authed_get("daily_usage", URL, {})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(m.call_count, 2)

    def test_does_not_retry_4xx(self):
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(URL, status_code=401, text="unauthorized")
            resp = authed_get("daily_usage", URL, {})

        self.assertEqual(resp.status_code, 401)
        self.assertEqual(m.call_count, 1)
        with self.assertRaises(requests.exceptions.HTTPError):
            resp.raise_for_status()


if __name__ == "__main__":
    unittest.main()
