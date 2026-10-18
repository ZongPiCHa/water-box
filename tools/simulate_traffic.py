#!/usr/bin/env python3
"""通过 water_community 业务接口模拟真实互动；一半行为是本地浏览，不发送浏览请求。"""

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
import heapq
import itertools
import json
import math
import random
import signal
import sys
import time
from urllib.parse import quote, urlsplit
import urllib.error
import urllib.request
import uuid


UNDO_RATE = 0.10
COMMENTS = [
    "这个构图很舒服", "光线太好看了", "想去这里看看", "颜色很高级，收藏了",
    "留白恰到好处", "每次看都有新发现", "氛围感拉满", "期待下一张作品",
]


@dataclass
class Action:
    action: str
    user_id: str
    project_id: str
    target_user_id: str
    headers: dict = field(default_factory=dict)
    comment_id: str | None = None
    content: str | None = None

    def request(self):
        project = quote(self.project_id, safe="")
        path = f"/api/projects/{project}"
        if self.action == "view_page":
            return None
        if self.action in ("like", "unlike"):
            return "POST", path + "/likes", None
        if self.action == "comment":
            return "POST", path + "/comments", {"content": self.content}
        if self.action == "delete_comment":
            if not self.comment_id:
                raise ValueError("删除评论必须使用接口返回的 comment_id")
            return "DELETE", path + "/comments/" + quote(self.comment_id, safe=""), None
        if self.action == "follow":
            return "POST", f"/api/users/{quote(self.target_user_id, safe='')}/follow?projectId={project}", None
        if self.action == "share":
            return "POST", path + "/shares", None
        raise ValueError(f"未知行为 {self.action}")

    def key(self):
        if self.action in ("like", "unlike"):
            return "like", self.user_id, self.project_id
        if self.action == "follow":
            return "follow", self.user_id, self.target_user_id
        return None


class CommunitySimulator:
    def __init__(self, catalog, seed=None):
        self.random = random.Random(seed)
        self.users = {
            user_id: {"last_seen": None, "session_id": str(uuid.uuid4()), "span_id": None}
            for user_id, _ in catalog["users"]
        }
        self.user_ids = list(self.users)
        self.projects = [(project, author) for project, author in catalog["projects"] if author in self.users]
        if len(self.users) < 2 or not self.projects:
            raise ValueError("社区至少需要 2 个用户和 1 个作品")
        self.user_weights = list(itertools.accumulate(
            self.random.lognormvariate(0, 0.8) for _ in self.user_ids))
        self.project_weights = list(itertools.accumulate(
            self.random.paretovariate(1.8) for _ in self.projects))
        self.likes = {tuple(pair) for pair in catalog.get("likes", [])}
        self.follows = {tuple(pair) for pair in catalog.get("follows", [])}
        self.inflight = set()
        # A failed toggle may have committed. Don't touch that pair again in this run.
        self.uncertain = set()
        self.undos = []
        self.serial = 0
        self.tick = 0

    def make_action(self, action, user_id, project_id, author, comment_id=None):
        user = self.users[user_id]
        now = time.monotonic()
        if user["last_seen"] is None or now - user["last_seen"] > 1800:
            user["session_id"] = str(uuid.uuid4())
            user["span_id"] = None
        user["last_seen"] = now
        if action == "view_page":
            user["span_id"] = str(uuid.uuid4())
        headers = {"X-User-Id": user_id, "X-Session-Id": user["session_id"]}
        if user["span_id"]:
            headers["X-Span-Id"] = user["span_id"]
        operation = Action(action, user_id, project_id, author, headers, comment_id,
                           self.random.choice(COMMENTS) if action == "comment" else None)
        if operation.key():
            self.inflight.add(operation.key())
        return operation

    def choose_pair(self):
        # O(log n) weighted sampling, no full catalog scan per operation.
        user = self.random.choices(self.user_ids, cum_weights=self.user_weights)[0]
        project, author = self.random.choices(self.projects, cum_weights=self.project_weights)[0]
        if user == author:
            # Also works for a catalog where every project belongs to the same author.
            index = self.user_ids.index(user)
            user = self.user_ids[(index + 1) % len(self.user_ids)]
        return user, project, author

    def next_action(self):
        self.tick += 1
        # Alternating slots keep exactly 50% browsing for even --count values, including undo operations.
        if self.tick % 2:
            return self.make_action("view_page", *self.choose_pair())
        while self.undos and self.undos[0][0] <= self.tick:
            _, _, original = heapq.heappop(self.undos)
            if original.key() in self.uncertain:
                continue
            return self.make_action(
                "unlike" if original.action == "like" else "delete_comment",
                original.user_id, original.project_id, original.target_user_id, original.comment_id)
        for _ in range(30):
            user, project, author = self.choose_pair()
            action = self.random.choices(["like", "comment", "share", "follow"], weights=[45, 30, 20, 5])[0]
            if action == "like":
                key = ("like", user, project)
                if (user, project) in self.likes or key in self.inflight or key in self.uncertain:
                    continue
            if action == "follow":
                key = ("follow", user, author)
                if (user, author) in self.follows or key in self.inflight or key in self.uncertain:
                    continue
            return self.make_action(action, user, project, author)
        # Share is repeatable; don't turn saturated mutation slots into extra views.
        return self.make_action("share", *self.choose_pair())

    def complete(self, operation, status, body, span_id=None):
        key = operation.key()
        self.inflight.discard(key)
        expected_status = {"like": 200, "unlike": 200, "follow": 200,
                           "comment": 201, "share": 201, "delete_comment": 204}
        success = status == expected_status[operation.action]
        if operation.action in ("like", "unlike", "follow", "share"):
            success = success and isinstance(body, dict) and body.get("active") is (operation.action != "unlike")
        if operation.action == "comment":
            success = (success and isinstance(body, dict)
                       and isinstance(body.get("id"), str) and bool(body["id"].strip()))
        if not success:
            if key:
                self.uncertain.add(key)
            return False
        if span_id:
            self.users[operation.user_id]["span_id"] = span_id
        if operation.action == "like":
            self.likes.add((operation.user_id, operation.project_id))
        elif operation.action == "unlike":
            self.likes.discard((operation.user_id, operation.project_id))
        elif operation.action == "follow":
            self.follows.add((operation.user_id, operation.target_user_id))
        elif operation.action == "comment":
            operation.comment_id = body["id"]
        # Only confirmed successful creations can schedule a delayed undo.
        if operation.action in ("like", "comment") and self.random.random() < UNDO_RATE:
            self.serial += 1
            heapq.heappush(self.undos, (self.tick + self.random.randint(5, 30), self.serial, operation))
        return True


def load_catalog(community_url, timeout=30):
    with urllib.request.urlopen(community_url.rstrip("/") + "/api/simulation/catalog", timeout=timeout) as response:
        return json.load(response)


def parse_arguments():
    parser = argparse.ArgumentParser(description="调用 water_community 真实业务接口，50% 浏览不发请求")
    parser.add_argument("--community-url", default="http://127.0.0.1:8090", help="线上 water_community 地址")
    parser.add_argument("--count", type=int, default=0, help="模拟行为总数（含浏览、取消）；0 持续运行")
    rate = parser.add_mutually_exclusive_group()
    rate.add_argument("--qps", type=float, default=100, help="每秒模拟行为数，默认 100；约 50 次业务请求/秒")
    rate.add_argument("--interval", type=float, help="行为间隔秒数，0 表示不限速")
    parser.add_argument("--concurrency", type=int, default=10, help="最多在途业务请求，默认 10")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--dry-run", action="store_true", help="读取目录并打印请求计划，不执行写请求，不安排取消")
    args = parser.parse_args()
    if (args.count < 0 or not math.isfinite(args.qps) or args.qps <= 0
            or (args.interval is not None and (not math.isfinite(args.interval) or args.interval < 0))
            or not math.isfinite(args.timeout) or args.timeout <= 0 or args.concurrency < 1):
        parser.error("count、interval 非负；qps、timeout、concurrency 必须为有限正数")
    parsed = urlsplit(args.community_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        parser.error("--community-url 必须为 http(s) 服务地址")
    return args


async def execute(session, base_url, operation):
    request = operation.request()
    if request is None:
        return None, None, None
    method, path, payload = request
    try:
        async with session.request(method, base_url.rstrip("/") + path, headers=operation.headers,
                                   json=payload, allow_redirects=False) as response:
            text = await response.text()
            try:
                body = json.loads(text) if text else None
            except ValueError:
                body = text[:200]
            return response.status, body, response.headers.get("X-Span-Id")
    except (OSError, asyncio.TimeoutError) as error:
        # aiohttp connection errors derive from OSError; other client errors are handled by the caller.
        return None, str(error), None


async def run_load(args, aiohttp, catalog, stop=None):
    simulator = CommunitySimulator(catalog, args.seed)
    stop = stop or asyncio.Event()
    counts = Counter()
    attempted = Counter()
    stats = {"behaviors": 0, "views": 0, "requests": 0, "success": 0, "failed": 0, "preview": 0}
    interval = args.interval if args.interval is not None else 1 / args.qps
    loop = asyncio.get_running_loop()
    started = last_report = next_due = loop.time()
    pending = set()
    print(f"真实社区：{args.community_url}，{len(simulator.users)} 用户、{len(simulator.projects)} 作品；"
          f"行为间隔 {interval:g}s，50% 本地浏览，并发上限 {args.concurrency}。"
          f"{'预览模式，不写数据库。' if args.dry_run else '互动会写入真实数据库；Ctrl+C 停止发起并等待在途请求。'}",
          file=sys.stderr, flush=True)

    async def submit(session, operation):
        try:
            status, body, span = await execute(session, args.community_url, operation)
        except aiohttp.ClientError as error:
            status, body, span = None, str(error), None
        if simulator.complete(operation, status, body, span):
            stats["success"] += 1
            counts[operation.action] += 1
        else:
            stats["failed"] += 1
            print(f"未确认成功（不重试）：action={operation.action} user={operation.user_id} "
                  f"project={operation.project_id} status={status} response={str(body)[:200]}",
                  file=sys.stderr, flush=True)

    timeout = aiohttp.ClientTimeout(total=args.timeout)
    async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=args.concurrency)) as session:
        while not stop.is_set() and (args.count == 0 or stats["behaviors"] < args.count):
            if len(pending) >= args.concurrency:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
                continue
            delay = next_due - loop.time()
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    pass
            if stop.is_set():
                break
            # Don't catch up missed slots with a burst after slow requests.
            next_due = loop.time() + interval
            operation = simulator.next_action()
            stats["behaviors"] += 1
            if operation.action == "view_page":
                stats["views"] += 1
                counts["view_page"] += 1
            elif not args.dry_run:
                stats["requests"] += 1
                attempted[operation.action] += 1
                pending.add(asyncio.create_task(submit(session, operation)))
            if args.dry_run:
                print(json.dumps({"action": operation.action, "user_id": operation.user_id,
                                  "project_id": operation.project_id, "request": operation.request()},
                                 ensure_ascii=False))
                simulator.inflight.discard(operation.key())
                stats["preview"] += 1
            done = {task for task in pending if task.done()}
            pending -= done
            for task in done:
                task.result()
            if interval == 0:
                # Allow HTTP tasks and stop signals to run even in unlimited-speed preview mode.
                await asyncio.sleep(0)
            if loop.time() - last_report >= 1:
                print(f"行为={stats['behaviors']} 浏览={stats['views']} HTTP={stats['requests']} "
                      f"成功={stats['success']} 未确认成功={stats['failed']} 在途={len(pending)}",
                      file=sys.stderr, flush=True)
                last_report = loop.time()
        if pending:
            await asyncio.gather(*pending)
    stats["pending_undos"] = len(simulator.undos)
    stats["actions"] = dict(counts)
    stats["attempted_actions"] = dict(attempted)
    print(f"完成：{json.dumps(stats, ensure_ascii=False)}，耗时 {loop.time() - started:.2f}s。"
          "待执行取消未强行补发；成功表示社区业务已提交，埋点投递结果由后端日志确认。",
          file=sys.stderr, flush=True)
    return stats


async def run_until_stopped(args, aiohttp, catalog):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.getsignal(sig)
        loop.add_signal_handler(sig, stop.set)
    try:
        return await run_load(args, aiohttp, catalog, stop)
    finally:
        for sig, handler in previous.items():
            loop.remove_signal_handler(sig)
            signal.signal(sig, handler)


def main():
    args = parse_arguments()
    try:
        import aiohttp
    except ModuleNotFoundError:
        print("缺少 aiohttp，请执行 python3 -m pip install -r tools/requirements.txt", file=sys.stderr)
        return 2
    try:
        catalog = load_catalog(args.community_url, args.timeout)
        result = asyncio.run(run_until_stopped(args, aiohttp, catalog))
        return 1 if result["failed"] else 0
    except (urllib.error.URLError, OSError, ValueError) as error:
        print(f"模拟器未能运行：{error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
