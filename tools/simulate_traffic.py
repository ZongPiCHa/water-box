#!/usr/bin/env python3
"""基于 water_community 真实数据的社区埋点模拟器，数据生成仅依赖 Python 标准库。"""

import argparse
import asyncio
from collections import Counter, deque
import heapq
import itertools
import json
import math
import random
import sys
import time
import urllib.error
import urllib.request
import uuid


ID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"
UNDO_RATE = 0.10
COMMENTS = [
    "这个构图很舒服", "光线太好看了", "想去这里看看", "颜色很高级，收藏了",
    "留白恰到好处", "每次看都有新发现", "氛围感拉满", "期待下一张作品",
]


class CommunitySimulator:
    """基于 water_community 真实用户、作品、点赞和关注关系生成后端埋点。"""

    def __init__(self, catalog, env="test", seed=None):
        self.random = random.Random(seed)
        self.env = env
        self.users = [
            {
                "user_id": user_id,
                "username": nickname,
                "session_id": self.new_id(),
                "last_seen": None,
                "activity": self.random.lognormvariate(0, 0.8),
            }
            for user_id, nickname in catalog["users"]
        ]
        users_by_id = {user["user_id"]: user for user in self.users}
        self.projects = [
            {
                "project_id": project_id,
                "author_user_id": author_id,
                "popularity": self.random.paretovariate(1.8),
            }
            for project_id, author_id in catalog["projects"]
            if author_id in users_by_id
        ]
        if len(self.users) < 2 or not self.projects:
            raise ValueError("社区至少需要 2 个用户和 1 个作品")
        self.project_weights = list(itertools.accumulate(
            project["popularity"] for project in self.projects
        ))
        self.user_weights = list(itertools.accumulate(user["activity"] for user in self.users))
        self.likes = {tuple(pair) for pair in catalog.get("likes", [])}
        self.follows = {tuple(pair) for pair in catalog.get("follows", [])}
        self.comments = {}
        self.pending = deque()
        self.undos = []
        self.tick = 0
        self.serial = 0

    @staticmethod
    def new_id():
        return str(uuid.uuid4())

    def new_business_id(self, prefix):
        # 与 water_community 一致：1 位类型前缀 + 15 位小写 base36，共 16 位。
        return prefix + "".join(self.random.choices(ID_ALPHABET, k=15))

    @staticmethod
    def now_ms():
        return time.time_ns() // 1_000_000

    def choose_project(self, user):
        # 热度加权抽样；累计权重配合二分查找，2 万作品时也是 O(log n)。
        for _ in range(10):
            project = self.random.choices(self.projects, cum_weights=self.project_weights)[0]
            if project["author_user_id"] != user["user_id"]:
                return project
        return next(
            project for project in self.projects
            if project["author_user_id"] != user["user_id"]
        )

    def event(self, user, system, action, properties, parent=None):
        return {
            "timestamp": self.now_ms(),
            "log_id": self.new_id(),
            "session_id": user["session_id"],
            "span_id": self.new_id(),
            "pre_span_id": parent,
            "system": system,
            "theme": "community",
            "action": action,
            "user_id": user["user_id"],
            "team_id": None,
            "username": user["username"],
            "env": self.env,
            "properties": properties,
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
                self.likes.discard((user["user_id"], properties["project_id"]))
            else:
                self.comments.pop(properties["comment_id"], None)
            user["last_seen"] = time.monotonic()
            # 取消点赞、删除评论是新的操作链路，沿用用户当前会话。
            return self.event(user, "backend", action, properties)

        user = self.random.choices(self.users, cum_weights=self.user_weights)[0]
        now = time.monotonic()
        if user["last_seen"] is None or now - user["last_seen"] > 1800:
            user["session_id"] = self.new_id()
        user["last_seen"] = now

        project = self.choose_project(user)
        properties = {
            "project_id": project["project_id"],
            "author_user_id": project["author_user_id"],
        }
        page = self.event(user, "front", "view_page", dict(properties))
        action = self.random.choices(
            ["view_page", "like", "comment", "share", "follow"],
            weights=[45, 25, 15, 10, 5],
        )[0]
        parent = page["span_id"]
        if action == "like":
            key = (user["user_id"], project["project_id"])
            if key not in self.likes:
                self.likes.add(key)
                self.pending.append(self.event(user, "backend", "like", dict(properties), parent))
                if self.random.random() < UNDO_RATE:
                    self.schedule_undo(user, "unlike", dict(properties))
        elif action == "comment":
            comment = {**properties, "comment_id": self.new_business_id("c")}
            self.comments[comment["comment_id"]] = user["user_id"]
            self.pending.append(self.event(user, "backend", "comment", comment, parent))
            if self.random.random() < UNDO_RATE:
                self.schedule_undo(user, "delete_comment", dict(comment))
        elif action == "share":
            self.pending.append(self.event(user, "backend", "share", dict(properties), parent))
        elif action == "follow":
            key = (user["user_id"], project["author_user_id"])
            if key not in self.follows:
                self.follows.add(key)
                self.pending.append(self.event(user, "backend", "follow", {
                    "project_id": project["project_id"],
                    "target_user_id": project["author_user_id"],
                }, parent))
        return page


def load_catalog(community_url, timeout=30):
    url = community_url.rstrip("/") + "/api/simulation/catalog"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def parse_arguments():
    parser = argparse.ArgumentParser(description="基于 water_community 真实用户和作品模拟社区埋点")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8080/api/v1/events")
    parser.add_argument(
        "--community-url", default="http://127.0.0.1:8090",
        help="water_community 服务地址，启动时读取用户、作品、点赞和关注关系",
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
    if (args.count < 0 or args.qps <= 0
            or (args.interval is not None and args.interval < 0)
            or args.timeout <= 0 or args.concurrency < 1):
        parser.error("count 和 interval 非负，qps、timeout 和 concurrency 为正")
    return args


async def run_load(args, aiohttp, catalog):
    simulator = CommunitySimulator(catalog, args.env, args.seed)
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
    print(f"模拟 {len(simulator.users)} 用户、{len(simulator.projects)} 作品，env={args.env}，"
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
        catalog = load_catalog(args.community_url)
    except (urllib.error.URLError, OSError, ValueError) as error:
        print(f"无法从 {args.community_url} 读取社区数据：{error}", file=sys.stderr)
        return 2
    try:
        return asyncio.run(run_load(args, aiohttp, catalog))
    except KeyboardInterrupt:
        print("\n已停止。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
