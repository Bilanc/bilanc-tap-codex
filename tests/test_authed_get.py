import time
import unittest
from email.utils import formatdate
from unittest import mock

import requests
import requests_mock

import tap_codex
from tap_codex import RetriableServerError, authed_get

URL = "https://api.chatgpt.com/v1/analytics/codex/workspaces/ws/usage"


@mock.patch("tap_codex.get_request_timeout", lambda: 1)
class AuthedGetRetryTest(unittest.TestCase):
    def setUp(self):
        sleep_patcher = mock.patch("time.sleep")
        self.sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

    def test_retries_any_5xx(self):
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(
                URL,
                [
                    {"status_code": 520, "text": "cloudflare"},
                    {"status_code": 599, "text": "proxy"},
                    {"status_code": 200, "json": {"data": []}},
                ],
            )
            resp = authed_get("daily_usage", URL, {})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(m.call_count, 3)

    def test_429_honours_retry_after(self):
        http_date = formatdate(time.time() + 45, usegmt=True)
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(
                URL,
                [
                    {"status_code": 429, "headers": {"Retry-After": "30"}},
                    {"status_code": 429, "headers": {"Retry-After": http_date}},
                    {"status_code": 429, "headers": {"Retry-After": "600"}},
                    {"status_code": 429, "headers": {"Retry-After": "garbage"}},
                    {"status_code": 200, "json": {"data": []}},
                ],
            )
            resp = authed_get("daily_usage", URL, {})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(m.call_count, 5)
        sleeps = [c.args[0] for c in self.sleep.call_args_list]
        self.assertEqual(len(sleeps), 4)
        self.assertEqual(sleeps[0], 30.0)
        self.assertTrue(40 <= sleeps[1] <= 45, sleeps[1])
        self.assertEqual(sleeps[2], float(tap_codex.MAX_BACKOFF_SECONDS))
        self.assertLessEqual(sleeps[3], 8)

    def test_does_not_sleep_after_final_attempt(self):
        with requests_mock.Mocker(session=tap_codex.session) as m:
            m.get(URL, status_code=429, headers={"Retry-After": "60"})
            with self.assertRaises(RetriableServerError):
                authed_get("daily_usage", URL, {})

        self.assertEqual(m.call_count, tap_codex.MAX_REQUEST_ATTEMPTS)
        self.assertEqual(self.sleep.call_count, tap_codex.MAX_REQUEST_ATTEMPTS - 1)

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
