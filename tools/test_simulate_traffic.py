"""验证模拟数据的跨事件业务关联，运行 python3 -m unittest discover -s tools。"""

from collections import Counter
import sys
import time
import unittest
from unittest.mock import patch

from simulate_traffic import CommunitySimulator, parse_arguments


class CommunitySimulatorTest(unittest.TestCase):
    def test_long_run_has_consistent_business_relationships(self):
        simulator = CommunitySimulator(seed=42)
        projects = dict(simulator.projects)
        likes = set()
        comments = {}
        follows = set()
        logs = set()
        spans = {}
        users = set()
        usernames = {}
        renamed = False
        counts = Counter()
        team_publications = 0
        for _ in range(20000):
            event = simulator.next_event()
            properties = event["properties"]
            action = event["action"]
            user = event["user_id"]
            users.add(user)
            counts[action] += 1
            self.assertNotIn(event["log_id"], logs)
            logs.add(event["log_id"])
            self.assertGreater(event["timestamp"], 0)
            self.assertEqual(event["env"], "test")
            self.assertTrue(event["session_id"])
            if event["pre_span_id"]:
                parent = spans[event["pre_span_id"]]
                self.assertEqual(parent["user_id"], user)
                self.assertEqual(parent["session_id"], event["session_id"])
                self.assertLessEqual(parent["timestamp"], event["timestamp"])
            spans[event["span_id"]] = event
            if user in usernames and usernames[user] != event["username"]:
                renamed = True
            usernames[user] = event["username"]

            if action == "publish":
                self.assertNotIn(properties["project_id"], projects)
                projects[properties["project_id"]] = {
                    **properties, "team_id": event["team_id"],
                }
                self.assertEqual(properties["author_user_id"], user)
                team_publications += event["team_id"] is not None
            if event["theme"] == "community":
                self.assertIn(properties["project_id"], projects)
                project = projects[properties["project_id"]]
                self.assertEqual(project["author_user_id"], properties["author_user_id"])
            key = (user, properties.get("project_id"))
            if action == "like":
                self.assertNotIn(key, likes)
                likes.add(key)
            elif action == "unlike":
                self.assertIn(key, likes)
                likes.remove(key)
            elif action == "comment":
                self.assertNotIn(properties["comment_id"], comments)
                comments[properties["comment_id"]] = key
            elif action == "delete_comment":
                self.assertEqual(comments.pop(properties["comment_id"]), key)
            elif action == "follow":
                target = properties["target_user_id"]
                self.assertNotEqual(target, user)
                self.assertNotIn((user, target), follows)
                follows.add((user, target))
            if action in {"like", "unlike", "comment", "delete_comment", "follow", "publish", "login"}:
                self.assertEqual(event["system"], "backend")
            elif action in {"view", "share"}:
                self.assertEqual(event["system"], "front")

        self.assertEqual(len(users), 100)
        self.assertTrue(renamed)
        self.assertGreater(team_publications, 0)
        for action in ["share", "follow", "login", "publish", "join", "update_username"]:
            self.assertGreater(counts[action], 0)
        self.assertLess(counts["login"], counts["like"])
        for undo, original in [("unlike", "like"), ("delete_comment", "comment")]:
            self.assertGreater(counts[undo] / counts[original], 0.07)
            self.assertLess(counts[undo] / counts[original], 0.13)

    def test_fixed_seed_repeats_actions_and_environments(self):
        left = CommunitySimulator(seed=123, env="prod")
        right = CommunitySimulator(seed=123, env="prod")
        for _ in range(500):
            a, b = left.next_event(), right.next_event()
            self.assertEqual((a["system"], a["theme"], a["action"]),
                             (b["system"], b["theme"], b["action"]))
            self.assertEqual(a["env"], "prod")

    def test_timestamp_is_unix_milliseconds(self):
        simulator = CommunitySimulator(seed=1)
        first = simulator.next_event()["timestamp"]
        time.sleep(0.003)
        second = simulator.next_event()["timestamp"]
        self.assertGreaterEqual(first, 1_000_000_000_000)
        self.assertLess(first, 10_000_000_000_000)
        self.assertGreaterEqual(second - first, 1)

    def test_default_load_is_two_thousand_qps(self):
        with patch.object(sys, "argv", ["simulate_traffic.py"]):
            arguments = parse_arguments()
        self.assertEqual(arguments.qps, 2000)
        self.assertEqual(arguments.concurrency, 200)

    def test_active_project_feed_is_bounded(self):
        simulator = CommunitySimulator(seed=7, max_active_projects=100)
        for _ in range(10000):
            simulator.next_event()
        self.assertEqual(len(simulator.projects), 100)
        self.assertEqual(len(simulator.project_feed), 100)


if __name__ == "__main__":
    unittest.main()
