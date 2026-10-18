"""python3 -m unittest discover -s tools; all HTTP tests use an isolated local fake community."""

import asyncio
from collections import Counter
import contextlib
import io
import sys
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp import web

from simulate_traffic import Action, CommunitySimulator, execute, parse_arguments, run_load


def catalog(users=10000, projects=20000):
    ids = [f"u{index:015d}" for index in range(1, users + 1)]
    return {
        "users": [[user, f"用户{index:05d}"] for index, user in enumerate(ids, 1)],
        "projects": [[f"p{index:015d}", ids[index % users]] for index in range(1, projects + 1)],
        "likes": [[ids[0], "p000000000000002"]],
        "follows": [[ids[0], ids[2]]] if users >= 3 else [],
    }


class CommunitySimulatorTest(unittest.TestCase):
    def test_real_ids_confirmed_state_and_fifty_percent_views_including_undos(self):
        data = catalog()
        simulator = CommunitySimulator(data, seed=42)
        likes = {tuple(pair) for pair in data["likes"]}
        follows = {tuple(pair) for pair in data["follows"]}
        comments = {}
        counts = Counter()
        for index in range(40000):
            operation = simulator.next_action()
            action = operation.action
            counts[action] += 1
            self.assertNotEqual(operation.user_id, operation.target_user_id)
            if action == "view_page":
                self.assertIsNone(operation.request())
                continue
            method, path, body = operation.request()
            self.assertEqual(operation.headers["X-User-Id"], operation.user_id)
            self.assertTrue(operation.headers["X-Session-Id"])
            self.assertNotIn("/events", path)
            pair = (operation.user_id, operation.project_id)
            if action == "like":
                self.assertNotIn(pair, likes)
                self.assertEqual((method, path), ("POST", f"/api/projects/{operation.project_id}/likes"))
                likes.add(pair)
            elif action == "unlike":
                self.assertIn(pair, likes)
                likes.remove(pair)
            elif action == "comment":
                self.assertIsNone(operation.comment_id)
                self.assertTrue(body["content"].strip())
                comment_id = f"c{index:015d}"  # Represents an ID returned by the real backend.
                comments[comment_id] = pair
                self.assertTrue(simulator.complete(operation, 201, {"id": comment_id}))
                continue
            elif action == "delete_comment":
                self.assertEqual(comments.pop(operation.comment_id), pair)
                self.assertEqual(method, "DELETE")
                self.assertTrue(path.endswith("/comments/" + operation.comment_id))
                self.assertTrue(simulator.complete(operation, 204, None))
                continue
            elif action == "follow":
                key = (operation.user_id, operation.target_user_id)
                self.assertNotIn(key, follows)
                follows.add(key)
                self.assertIn("?projectId=" + operation.project_id, path)
            self.assertTrue(simulator.complete(operation, 201 if action == "share" else 200,
                                               {"active": action != "unlike"}))
        self.assertEqual(counts["view_page"], 20000)
        self.assertEqual(set(counts), {"view_page", "like", "unlike", "comment", "delete_comment", "share", "follow"})
        self.assertEqual(likes, simulator.likes)
        self.assertEqual(follows, simulator.follows)
        for undo, creation in (("unlike", "like"), ("delete_comment", "comment")):
            self.assertGreater(counts[undo] / counts[creation], 0.07)
            self.assertLess(counts[undo] / counts[creation], 0.13)

    def test_failed_or_inflight_toggle_cannot_be_sent_again(self):
        data = catalog(2, 1)
        data["likes"] = []
        simulator = CommunitySimulator(data, seed=1)
        user = data["users"][0][0]
        project, author = data["projects"][0]
        operation = simulator.make_action("like", user, project, author)
        self.assertNotIn((user, project), simulator.likes)
        for _ in range(100):
            candidate = simulator.next_action()
            self.assertNotEqual(candidate.action, "like")
        self.assertFalse(simulator.complete(operation, None, "timed out"))
        self.assertIn(operation.key(), simulator.uncertain)
        self.assertFalse(simulator.undos)
        for _ in range(100):
            self.assertNotEqual(simulator.next_action().action, "like")

    def test_delete_uses_successful_comment_response_only(self):
        simulator = CommunitySimulator(catalog(10, 20), seed=1)
        operation = simulator.make_action("comment", *simulator.choose_pair())
        with patch.object(simulator.random, "random", return_value=0):
            self.assertFalse(simulator.complete(operation, 500, {"id": "not-confirmed"}))
            self.assertFalse(simulator.undos)
            self.assertTrue(simulator.complete(operation, 201, {"id": "cbackend00000001"}))
        simulator.tick = 999
        deletion = simulator.next_action()
        self.assertEqual(deletion.action, "delete_comment")
        self.assertEqual(deletion.comment_id, "cbackend00000001")
        self.assertEqual(deletion.user_id, operation.user_id)

    def test_unexpected_toggle_response_is_not_counted_as_intended_success(self):
        simulator = CommunitySimulator(catalog(10, 20))
        operation = simulator.make_action("follow", *simulator.choose_pair())
        self.assertFalse(simulator.complete(operation, 200, {"active": False}))
        self.assertIn(operation.key(), simulator.uncertain)

    def test_defaults_and_input_validation(self):
        with patch.object(sys, "argv", ["simulate_traffic.py"]):
            args = parse_arguments()
        self.assertEqual(args.qps, 100)
        self.assertEqual(args.concurrency, 10)
        self.assertFalse(hasattr(args, "endpoint"))
        self.assertFalse(hasattr(args, "env"))
        for flag, value in [("--qps", "nan"), ("--qps", "0"), ("--timeout", "inf"), ("--count", "-1")]:
            with patch.object(sys, "argv", ["simulate_traffic.py", flag, value]), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parse_arguments()


class HttpSimulationTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.comments = {}
        self.likes = set()
        self.follows = set()
        self.serial = 0
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self.handler)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def handler(self, request):
        self.requests.append((request.method, request.path))
        user = request.headers["X-User-Id"]
        self.assertTrue(request.headers["X-Session-Id"])
        parts = request.path.split("/")
        headers = {"X-Span-Id": "backend-span"}
        if request.path.endswith("/likes"):
            key = user, parts[3]
            active = key not in self.likes
            self.likes.add(key) if active else self.likes.remove(key)
            return web.json_response({"active": active}, headers=headers)
        if request.path.endswith("/follow"):
            self.assertTrue(request.query["projectId"])
            key = user, parts[3]
            self.assertNotIn(key, self.follows)
            self.follows.add(key)
            return web.json_response({"active": True}, headers=headers)
        if request.path.endswith("/shares"):
            return web.json_response({"active": True}, status=201, headers=headers)
        if request.method == "POST" and request.path.endswith("/comments"):
            self.assertTrue((await request.json())["content"])
            self.serial += 1
            comment_id = f"c{self.serial:015d}"
            self.comments[comment_id] = (user, parts[3])
            return web.json_response({"id": comment_id}, status=201, headers=headers)
        if request.method == "DELETE":
            self.assertEqual(self.comments.pop(parts[5]), (user, parts[3]))
            return web.Response(status=204)
        self.fail("Unexpected API " + request.path)

    def args(self, extra=()):
        with patch.object(sys, "argv", ["simulate_traffic.py", "--community-url", self.url,
                                       "--count", "1000", "--interval", "0", "--seed", "42", *extra]):
            return parse_arguments()

    async def test_live_http_mapping_no_view_requests_and_valid_comment_deletes(self):
        data = catalog(100, 200)
        self.likes = {tuple(pair) for pair in data["likes"]}
        self.follows = {tuple(pair) for pair in data["follows"]}
        with contextlib.redirect_stderr(io.StringIO()):
            stats = await run_load(self.args(), aiohttp, data)
        self.assertEqual(stats["views"], 500)
        self.assertEqual(stats["requests"], 500)
        self.assertEqual(stats["success"], 500)
        self.assertEqual(stats["failed"], 0)
        self.assertEqual(len(self.requests), 500)
        for action in ("like", "unlike", "comment", "delete_comment", "share", "follow"):
            self.assertGreater(stats["actions"][action], 0)
        self.assertFalse(any("/views" in path or "/events" in path for _, path in self.requests))

    async def test_views_and_dry_run_never_send_http(self):
        self.assertEqual(await execute(None, self.url, Action("view_page", "u", "p", "a")), (None, None, None))
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            stats = await run_load(self.args(["--dry-run", "--count", "100"]), aiohttp, catalog(10, 20))
        self.assertEqual(self.requests, [])
        self.assertEqual(stats["requests"], 0)
        self.assertEqual(stats["views"], 50)
        self.assertEqual(stats["pending_undos"], 0)

    async def test_failed_requests_are_not_retried(self):
        async def failed(*args):
            return None, "timeout after a possible commit", None
        with patch("simulate_traffic.execute", side_effect=failed) as send, contextlib.redirect_stderr(io.StringIO()):
            stats = await run_load(self.args(["--count", "100"]), aiohttp, catalog(10, 20))
        self.assertEqual(send.call_count, 50)
        self.assertEqual(stats["failed"], 50)
        self.assertEqual(stats["pending_undos"], 0)

    async def test_rate_limit_and_graceful_stop_drain_inflight(self):
        stop = asyncio.Event()
        async def slow(*args):
            stop.set()
            await asyncio.sleep(0.03)
            return 201, {"id": "creal00000000001", "active": True}, None
        with patch("simulate_traffic.execute", side_effect=slow) as send, contextlib.redirect_stderr(io.StringIO()):
            stats = await run_load(self.args(["--count", "100"]), aiohttp, catalog(10, 20), stop)
        self.assertEqual(send.call_count, 1)
        self.assertEqual(stats["requests"], stats["success"] + stats["failed"])
        # A finite run cannot emit a catch-up burst.
        started = asyncio.get_running_loop().time()
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            await run_load(self.args(["--count", "6", "--interval", "0.02", "--dry-run"]), aiohttp, catalog(10, 20))
        self.assertGreaterEqual(asyncio.get_running_loop().time() - started, 0.095)


if __name__ == "__main__":
    unittest.main()
