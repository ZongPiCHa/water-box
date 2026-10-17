"""验证模拟数据的跨事件业务关联，运行 python3 -m unittest discover -s tools。"""

from collections import Counter
import re
import sys
import time
import unittest
from unittest.mock import patch

from simulate_traffic import CommunitySimulator, parse_arguments

ACTIONS = {"view_page", "like", "unlike", "comment", "delete_comment", "share", "follow"}
REQUIRED_PROPERTIES = {
    "view_page": {"project_id", "author_user_id"},
    "like": {"project_id", "author_user_id"},
    "unlike": {"project_id", "author_user_id"},
    "share": {"project_id", "author_user_id"},
    "comment": {"project_id", "author_user_id", "comment_id"},
    "delete_comment": {"project_id", "author_user_id", "comment_id"},
    "follow": {"project_id", "target_user_id"},
}
ID_PATTERN = re.compile(r"^[a-z][0-9a-z]{15}$")


def catalog(users=10000, projects=20000):
    """与扩容后的 water_community 同规模的用户、作品、已有点赞和关注。"""
    user_ids = [f"u{index:015d}" for index in range(1, users + 1)]
    return {
        "users": [[user_id, f"用户{index:05d}"] for index, user_id in enumerate(user_ids, 1)],
        "projects": [
            [f"p{index:015d}", user_ids[index % users]] for index in range(1, projects + 1)
        ],
        "likes": [[user_ids[0], "p000000000000002"]],
        "follows": [[user_ids[0], user_ids[2]]],
    }


class CommunitySimulatorTest(unittest.TestCase):
    def test_long_run_matches_community_data_and_action_contract(self):
        data = catalog()
        simulator = CommunitySimulator(data, seed=42)
        authors = {project_id: author for project_id, author in data["projects"]}
        users = {user_id for user_id, _ in data["users"]}
        likes = {tuple(pair) for pair in data["likes"]}
        follows = {tuple(pair) for pair in data["follows"]}
        comments = {}
        logs = set()
        spans = {}
        counts = Counter()
        for _ in range(40000):
            event = simulator.next_event()
            properties = event["properties"]
            action = event["action"]
            user = event["user_id"]
            counts[action] += 1
            self.assertIn(action, ACTIONS)
            self.assertEqual(set(properties), REQUIRED_PROPERTIES[action])
            for value in properties.values():
                self.assertIsInstance(value, str)
                self.assertRegex(value, ID_PATTERN)
            self.assertIn(user, users)
            self.assertEqual(event["theme"], "community")
            self.assertEqual(event["system"], "front" if action == "view_page" else "backend")
            self.assertIsNone(event["team_id"])
            self.assertNotIn(event["log_id"], logs)
            logs.add(event["log_id"])
            if event["pre_span_id"]:
                parent = spans[event["pre_span_id"]]
                self.assertEqual(parent["action"], "view_page")
                self.assertEqual(parent["user_id"], user)
                self.assertEqual(parent["session_id"], event["session_id"])
                self.assertEqual(parent["properties"]["project_id"], properties["project_id"])
            spans[event["span_id"]] = event

            project_id = properties["project_id"]
            self.assertIn(project_id, authors)
            if "author_user_id" in properties:
                self.assertEqual(properties["author_user_id"], authors[project_id])
                self.assertNotEqual(properties["author_user_id"], user)
            key = (user, project_id)
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
                self.assertEqual(target, authors[project_id])
                self.assertNotIn((user, target), follows)
                follows.add((user, target))

        self.assertEqual(set(counts), ACTIONS)
        for undo, original in [("unlike", "like"), ("delete_comment", "comment")]:
            self.assertGreater(counts[undo] / counts[original], 0.07)
            self.assertLess(counts[undo] / counts[original], 0.13)

    def test_fixed_seed_repeats_actions_and_environments(self):
        data = catalog(100, 200)
        left = CommunitySimulator(data, seed=123, env="prod")
        right = CommunitySimulator(data, seed=123, env="prod")
        for _ in range(500):
            a, b = left.next_event(), right.next_event()
            self.assertEqual((a["system"], a["action"], a["user_id"], a["properties"]),
                             (b["system"], b["action"], b["user_id"], b["properties"]))
            self.assertEqual(a["env"], "prod")

    def test_timestamp_is_unix_milliseconds(self):
        simulator = CommunitySimulator(catalog(10, 20), seed=1)
        first = simulator.next_event()["timestamp"]
        time.sleep(0.003)
        second = simulator.next_event()["timestamp"]
        self.assertGreaterEqual(first, 1_000_000_000_000)
        self.assertLess(first, 10_000_000_000_000)
        self.assertGreaterEqual(second - first, 1)

    def test_requires_users_and_projects(self):
        with self.assertRaises(ValueError):
            CommunitySimulator({"users": [["u000000000000001", "a"]], "projects": []})

    def test_default_load_is_two_thousand_qps(self):
        with patch.object(sys, "argv", ["simulate_traffic.py"]):
            arguments = parse_arguments()
        self.assertEqual(arguments.qps, 2000)
        self.assertEqual(arguments.concurrency, 200)
        self.assertEqual(arguments.community_url, "http://127.0.0.1:8090")


if __name__ == "__main__":
    unittest.main()
