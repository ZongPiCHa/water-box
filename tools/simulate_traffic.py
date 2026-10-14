#!/usr/bin/env python3
"""有状态的社区埋点模拟器，仅依赖 Python 标准库。"""

import argparse
import asyncio
from collections import Counter, deque
import heapq
import json
import math
import random
import sys
import time
import uuid


class CommunitySimulator:
    def __init__(self, users=100, env="test", seed=None, max_active_projects=10000):
        self.random = random.Random(seed)
        self.env = env
        self.run_id = uuid.uuid4().hex[:8]
        self.users = []
        self.projects = {}
        self.project_feed = []
        self.project_feed_cursor = 0
        self.max_active_projects = max_active_projects
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

    @staticmethod
    def now_ms():
        return time.time_ns() // 1_000_000

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
        if len(self.project_feed) < self.max_active_projects:
            self.project_feed.append(project)
        else:
            expired = self.project_feed[self.project_feed_cursor]
            self.projects.pop(expired["project_id"], None)
            self.project_feed[self.project_feed_cursor] = project
            self.project_feed_cursor = (
                self.project_feed_cursor + 1
            ) % self.max_active_projects
        return project

    def choose_project(self, user):
        # 从活跃 Feed 的固定小样本中按热度选择，避免作品总量增长后逐条全表扫描。
        sample_size = min(16, len(self.project_feed))
        for _ in range(5):
            candidates = self.random.sample(self.project_feed, sample_size)
            candidates = [
                project for project in candidates
                if project["author_user_id"] != user["user_id"]
            ]
            if candidates:
                return self.random.choices(
                    candidates,
                    weights=[project["popularity"] for project in candidates],
                )[0]
        return next(
            project for project in self.project_feed
            if project["author_user_id"] != user["user_id"]
        )

    def event(self, user, system, theme, action, properties, parent=None, team_id=None):
        return {
            "timestamp": self.now_ms(),
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
            event["timestamp"] = self.now_ms()
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

        project = self.choose_project(user)
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
    parser.add_argument(
        "--max-active-projects", type=int, default=10000,
        help="参与后续互动的活跃作品池上限",
    )
    parser.add_argument("--count", type=int, default=0, help="发送条数，0 表示持续运行")
    rate = parser.add_mutually_exclusive_group()
    rate.add_argument("--qps", type=float, default=2000, help="每秒请求尝试次数，默认 2000")
    rate.add_argument("--interval", type=float, help="请求发起间隔秒数，0 表示不限速")
    parser.add_argument("--concurrency", type=int, default=200, help="最大并发请求数")
    parser.add_argument("--timeout", type=float, default=35)
    parser.add_argument("--env", choices=["prod", "test"], default="test")
    parser.add_argument("--seed", type=int, help="固定行为随机种子")
    parser.add_argument("--dry-run", action="store_true", help="仅打印 JSON，不调用服务")
    args = parser.parse_args()
    if (args.users < 2 or args.max_active_projects < 100 or args.count < 0 or args.qps <= 0
            or (args.interval is not None and args.interval < 0)
            or args.timeout <= 0 or args.concurrency < 1):
        parser.error("users 至少为 2，活跃作品池至少为 100，count 和 interval 非负，"
                     "qps、timeout 和 concurrency 为正")
    return args


async def run_load(args, aiohttp):
    simulator = CommunitySimulator(
        args.users, args.env, args.seed, args.max_active_projects
    )
    successes = failed_attempts = recovered = attempts = 0
    actions = Counter()
    target_qps = args.qps if args.interval is None else (
        None if args.interval == 0 else 1 / args.interval
    )
    interval = 0 if target_qps is None else 1 / target_qps
    burst_limit = args.concurrency if target_qps is None else max(1, math.ceil(target_qps * 0.01))
    pending = set()
    completed = asyncio.Queue()
    retries = deque()
    retrying_log_ids = set()
    loop = asyncio.get_running_loop()
    started = last_report = loop.time()
    reported_attempts = 0
    print(f"模拟 {args.users} 用户，env={args.env}，"
          f"发送间隔={interval:g}s，并发上限={args.concurrency}，Ctrl+C 停止。",
          file=sys.stderr, flush=True)

    async def send_event(session, event):
        try:
            async with session.post(args.endpoint, json=event) as response:
                return response.status, await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
            return None, str(error)

    def collect_results():
        nonlocal successes, failed_attempts, recovered
        while True:
            try:
                task, event = completed.get_nowait()
            except asyncio.QueueEmpty:
                break
            pending.discard(task)
            status, body = task.result()
            if status == 202:
                successes += 1
                actions[event["action"]] += 1
                if event["log_id"] in retrying_log_ids:
                    retrying_log_ids.remove(event["log_id"])
                    recovered += 1
            else:
                failed_attempts += 1
                retrying_log_ids.add(event["log_id"])
                retries.append(event)
                print(f"失败尝试：log_id={event['log_id']} status={status} response={body}",
                      file=sys.stderr, flush=True)

    timeout = aiohttp.ClientTimeout(total=args.timeout)
    connector = aiohttp.TCPConnector(
        limit=args.concurrency,
        limit_per_host=args.concurrency,
        keepalive_timeout=30,
    )
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while args.count == 0 or attempts < args.count:
            collect_results()
            now = loop.time()
            if now - last_report >= 1:
                print(f"发送速率={(attempts - reported_attempts) / (now - last_report):.1f}/s "
                      f"累计发送={attempts} 成功={successes} 失败尝试={failed_attempts} "
                      f"已恢复={recovered} 未恢复={len(retrying_log_ids)} "
                      f"在途={len(pending)}",
                      file=sys.stderr, flush=True)
                last_report, reported_attempts = now, attempts

            capacity = args.concurrency - len(pending)
            remaining = capacity if args.count == 0 else min(capacity, args.count - attempts)
            if target_qps is None:
                due = remaining
            else:
                expected = math.floor((now - started) * target_qps) + 1
                due = min(max(0, expected - attempts), burst_limit, remaining)

            for _ in range(due):
                event = retries.popleft() if retries else simulator.next_event()
                event["timestamp"] = simulator.now_ms()
                if args.dry_run:
                    print(json.dumps(event, ensure_ascii=False), flush=True)
                    successes += 1
                    actions[event["action"]] += 1
                else:
                    task = asyncio.create_task(send_event(session, event))
                    pending.add(task)
                    task.add_done_callback(
                        lambda finished, submitted=event:
                        completed.put_nowait((finished, submitted))
                    )
                attempts += 1

            if due == 0:
                await asyncio.sleep(0.0005)

        if pending:
            await asyncio.gather(*pending)
        collect_results()
    print(f"完成：发送 {attempts}，成功 {successes}，失败尝试 {failed_attempts}，"
          f"已恢复 {recovered}，未恢复 {len(retrying_log_ids)}，"
          f"耗时 {loop.time() - started:.2f}s，行为统计 {dict(actions)}",
          file=sys.stderr)
    return 1 if retrying_log_ids else 0


def main():
    args = parse_arguments()
    try:
        import aiohttp
    except ModuleNotFoundError:
        print("缺少 aiohttp，请先执行：python3 -m pip install -r tools/requirements.txt",
              file=sys.stderr)
        return 2
    try:
        return asyncio.run(run_load(args, aiohttp))
    except KeyboardInterrupt:
        print("\n已停止。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
