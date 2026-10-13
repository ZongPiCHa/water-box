#!/usr/bin/env python3
"""有状态的社区埋点模拟器，仅依赖 Python 标准库。"""

import argparse
from collections import Counter, deque
import heapq
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid


class CommunitySimulator:
    def __init__(self, users=100, env="test", seed=None):
        self.random = random.Random(seed)
        self.env = env
        self.run_id = uuid.uuid4().hex[:8]
        self.users = []
        self.projects = {}
        self.comments = {}
        self.likes = set()
        self.follows = set()
        self.pending = deque()
        self.undos = []
        self.tick = 0
        self.serial = 0
        self.project_serial = 0
        self.comment_serial = 0
        for index in range(users):
            self.users.append({
                "user_id": f"user-{self.run_id}-{index + 1:03d}",
                "username": f"designer_{index + 1:03d}",
                "team_id": f"team-{self.run_id}-{index % 8 + 1}" if self.random.random() < 0.35 else None,
                "session_id": self.new_id(),
                "last_seen": None,
                "activity": self.random.lognormvariate(0, 0.8),
                "location": self.random.choice(["america", "china", "germany", "japan"]),
                "device": self.random.choice(["desktop", "desktop", "mobile"]),
            })
        # 初始已有作品，便于第一条事件就能模拟浏览和互动。
        for _ in range(max(20, users // 2)):
            self.create_project(self.random.choice(self.users))

    @staticmethod
    def new_id():
        return str(uuid.uuid4())

    def create_project(self, user):
        self.project_serial += 1
        project_id = f"project-{self.run_id}-{self.project_serial:05d}"
        project = {
            "project_id": project_id,
            "author_user_id": user["user_id"],
            "team_id": user["team_id"] if self.random.random() < 0.45 else None,
            "title": self.random.choice(["Dashboard UI", "Mobile App Kit", "Portfolio", "Design System"]),
            "popularity": self.random.paretovariate(1.8),
        }
        self.projects[project_id] = project
        return project

    def event(self, user, system, theme, action, properties, parent=None, team_id=None):
        return {
            "timestamp": int(time.time() * 1000),
            "log_id": self.new_id(),
            "session_id": user["session_id"],
            "span_id": self.new_id(),
            "pre_span_id": parent,
            "system": system,
            "theme": theme,
            "action": action,
            "user_id": user["user_id"],
            "team_id": team_id,
            "username": user["username"],
            "env": self.env,
            "properties": {
                "source": "simulate_traffic.py",
                "simulation_run_id": self.run_id,
                "location": user["location"],
                "device": user["device"],
                **properties,
            },
        }

    def schedule_undo(self, user, action, properties):
        self.serial += 1
        heapq.heappush(self.undos, (
            self.tick + self.random.randint(5, 30), self.serial, user, action, properties,
        ))

    def next_event(self):
        self.tick += 1
        if self.pending:
            event = self.pending.popleft()
            event["timestamp"] = int(time.time() * 1000)
            return event

        if self.undos and self.undos[0][0] <= self.tick:
            _, _, user, action, properties = heapq.heappop(self.undos)
            if action == "unlike":
                self.likes.remove((user["user_id"], properties["project_id"]))
            else:
                self.comments.pop(properties["comment_id"])
            user["last_seen"] = time.monotonic()
            # 删除/取消是新的操作链路，沿用用户当前会话。
            return self.event(user, "backend", "community", action, properties)

        user = self.random.choices(self.users, weights=[u["activity"] for u in self.users])[0]
        now = time.monotonic()
        if user["last_seen"] is None or now - user["last_seen"] > 1800:
            user["session_id"] = self.new_id()
        user["last_seen"] = now
        choice = self.random.random()

        if choice < 0.025:
            user["session_id"] = self.new_id()
            page = self.event(user, "front", "login_page", "view", {"page": "/login"})
            self.pending.append(self.event(
                user, "backend", "login_page", "login",
                {"method": self.random.choice(["password", "google", "github"]), "success": True},
                parent=page["span_id"],
            ))
            return page

        if choice < 0.035:
            if self.random.random() < 0.5:
                old_name = user["username"]
                user["username"] = f"designer_{self.random.randint(1000, 9999)}"
                return self.event(user, "backend", "profile", "update_username", {
                    "previous_username": old_name,
                })
            user["team_id"] = f"team-{self.run_id}-{self.random.randint(1, 8)}"
            return self.event(user, "backend", "team", "join", {}, team_id=user["team_id"])

        if choice < 0.065:
            project = self.create_project(user)
            return self.event(user, "backend", "community", "publish", {
                "project_id": project["project_id"],
                "author_user_id": project["author_user_id"],
                "title": project["title"],
            }, team_id=project["team_id"])

        candidates = [p for p in self.projects.values() if p["author_user_id"] != user["user_id"]]
        project = self.random.choices(candidates, weights=[p["popularity"] for p in candidates])[0]
        properties = {
            "project_id": project["project_id"],
            "author_user_id": project["author_user_id"],
        }
        page = self.event(user, "front", "community", "view", {
            **properties,
            "page": f"/community/project/{project['project_id']}",
            "duration": self.random.randint(800, 90000),
            "referrer": self.random.choice(["feed", "search", "profile", "shared_link"]),
        })
        action = self.random.choices(
            ["view", "like", "comment", "share", "follow"],
            weights=[45, 25, 15, 10, 5],
        )[0]
        parent = page["span_id"]
        if action == "like":
            key = (user["user_id"], project["project_id"])
            if key not in self.likes:
                self.likes.add(key)
                self.pending.append(self.event(user, "backend", "community", "like", properties, parent))
                if self.random.random() < 0.10:
                    self.schedule_undo(user, "unlike", dict(properties))
        elif action == "comment":
            self.comment_serial += 1
            comment_id = f"comment-{self.run_id}-{self.comment_serial:06d}"
            comment = {
                **properties,
                "comment_id": comment_id,
                "content": self.random.choice([
                    "这个组件很实用！", "配色很好看，感谢分享", "Great work!",
                    "布局很清晰，期待更多作品", "Nice typography!",
                ]),
            }
            self.comments[comment_id] = user["user_id"]
            self.pending.append(self.event(user, "backend", "community", "comment", comment, parent))
            if self.random.random() < 0.10:
                self.schedule_undo(user, "delete_comment", {
                    **properties, "comment_id": comment_id,
                })
        elif action == "share":
            self.pending.append(self.event(user, "front", "community", "share", {
                **properties,
                "share_url": f"https://community.example.com/project/{project['project_id']}?ref={user['user_id']}",
                "channel": self.random.choice(["copy_link", "wechat", "twitter"]),
            }, parent))
        elif action == "follow":
            key = (user["user_id"], project["author_user_id"])
            if key not in self.follows:
                self.follows.add(key)
                self.pending.append(self.event(user, "backend", "community", "follow", {
                    **properties, "target_user_id": project["author_user_id"],
                }, parent))
        return page


def parse_arguments():
    parser = argparse.ArgumentParser(description="模拟 100 用户在类 Figma 社区中的行为")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8080/api/v1/events")
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--count", type=int, default=0, help="发送条数，0 表示持续运行")
    parser.add_argument("--interval", type=float, default=5, help="每条响应后的等待秒数")
    parser.add_argument("--timeout", type=float, default=35)
    parser.add_argument("--env", choices=["prod", "test"], default="test")
    parser.add_argument("--seed", type=int, help="固定行为随机种子")
    parser.add_argument("--dry-run", action="store_true", help="仅打印 JSON，不调用服务")
    args = parser.parse_args()
    if args.users < 2 or args.count < 0 or args.interval < 0 or args.timeout <= 0:
        parser.error("users 至少为 2，count 和 interval 非负，timeout 为正")
    return args


def send_event(endpoint, event, timeout):
    request = urllib.request.Request(
        endpoint, data=json.dumps(event, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return None, str(error)


def main():
    args = parse_arguments()
    simulator = CommunitySimulator(args.users, args.env, args.seed)
    successes = failures = attempts = 0
    actions = Counter()
    event = None
    print(f"模拟 {args.users} 用户，env={args.env}，间隔={args.interval:g}s，Ctrl+C 停止。",
          file=sys.stderr, flush=True)
    try:
        while args.count == 0 or attempts < args.count:
            # 失败后重试同一条，保持 log_id 和业务关联，不跳过前置操作。
            if event is None:
                event = simulator.next_event()
            attempts += 1
            if args.dry_run:
                print(json.dumps(event, ensure_ascii=False), flush=True)
                status = 202
            else:
                status, body = send_event(args.endpoint, event, args.timeout)
                print(f"[{attempts}] {event['system']}/{event['theme']}/{event['action']} "
                      f"user={event['user_id']} log_id={event['log_id']} "
                      f"status={status} response={body}", flush=True)
            if status == 202:
                successes += 1
                actions[event["action"]] += 1
                event = None
            else:
                failures += 1
            if args.count == 0 or attempts < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n已停止。", file=sys.stderr)
    print(f"完成：成功 {successes}，失败尝试 {failures}，行为统计 {dict(actions)}",
          file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
