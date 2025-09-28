#!/usr/bin/env python3
"""
Fetch all GitHub repositories with more than N stars using the Search API,
working around the 1,000-result pagination cap by partitioning the results
into star ranges.

- The GitHub Search API has a hard limit of 1,000 results per query. This
  script partitions by star counts to ensure each sub-query returns <= 1,000,
  then iterates through all sub-queries to collect the full set.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, AsyncIterator
import asyncio
import aiohttp
import urllib.parse as urlparse


@dataclass
class RateLimitInfo:
    remaining: Optional[int]
    reset_epoch: Optional[int]


class FetchGithubRepos:
    GITHUB_API_BASE = "https://api.github.com"
    SEARCH_REPOS_ENDPOINT = f"{GITHUB_API_BASE}/search/repositories"

    def __init__(self, token, max_repos, min_stars = 250, out_path: Optional[str] = None, concurrency: int = 10):
        self.token = token
        self.min_stars = min_stars
        self.max_repos = max_repos
        self.out_path = out_path or f"repos_over_{min_stars}.jsonl"
        self.concurrency = max(1, int(concurrency))

    def _default_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "coregrep-repo-fetcher/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def github_request(self, url: str) -> Tuple[dict, Dict[str, str]]:
        # Legacy sync method retained (unused in async flow). Kept for reference/backward-compat.
        import urllib.request as urlrequest  # local import to avoid unused at module level
        from urllib.error import HTTPError, URLError

        headers = self._default_headers()
        req = urlrequest.Request(url, headers=headers)

        try:
            with urlrequest.urlopen(req) as resp:
                body_bytes = resp.read()
                body_text = body_bytes.decode("utf-8")
                data = json.loads(body_text)
                resp_headers = {k: v for k, v in resp.headers.items()}
                return data, resp_headers
        except HTTPError as e:
            if e.code in (403, 429):
                resp_headers = {k: v for k, v in (e.headers.items() if e.headers else [])}
                self.maybe_sleep_for_rate_limit(resp_headers, minimum_remaining=1)
                with urlrequest.urlopen(req) as resp:
                    body_bytes = resp.read()
                    body_text = body_bytes.decode("utf-8")
                    data = json.loads(body_text)
                    resp_headers = {k: v for k, v in resp.headers.items()}
                    return data, resp_headers
            raise
        except URLError:
            time.sleep(1.5)
            with urlrequest.urlopen(req) as resp:
                body_bytes = resp.read()
                body_text = body_bytes.decode("utf-8")
                data = json.loads(body_text)
                resp_headers = {k: v for k, v in resp.headers.items()}
            return data, resp_headers

    async def async_github_request(self, session: aiohttp.ClientSession, url: str) -> Tuple[dict, Dict[str, str]]:
        attempt = 0
        backoff_seconds = 1.0
        while True:
            try:
                async with session.get(url, headers=self._default_headers()) as resp:
                    # Handle rate limiting
                    if resp.status in (403, 429):
                        headers = {k: v for k, v in resp.headers.items()}
                        await self.maybe_sleep_for_rate_limit_async(headers, minimum_remaining=1)
                        # retry immediately after sleep
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                    headers = {k: v for k, v in resp.headers.items()}
                    return data, headers
            except aiohttp.ClientResponseError as e:
                if e.status in (500, 502, 503, 504):
                    await asyncio.sleep(backoff_seconds)
                    attempt += 1
                    backoff_seconds = min(30.0, backoff_seconds * 2)
                    continue
                raise
            except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError):
                await asyncio.sleep(backoff_seconds)
                attempt += 1
                backoff_seconds = min(30.0, backoff_seconds * 2)
                continue


    def search_total_count(self, star_filter: str) -> Tuple[int, Dict[str, str]]:
        params = {
            "q": f"stars:{star_filter}",
            "per_page": "1",
        }

        url = f"{self.SEARCH_REPOS_ENDPOINT}?{urlparse.urlencode(params)}"
        data, headers = self.github_request(url)

        total_count = int(data.get("total_count", 0))

        print("[+] Total count search ", total_count)

        return total_count, headers


    def parse_rate_limit(self, headers: Dict[str, str]) -> RateLimitInfo:
        remaining_str = headers.get("X-RateLimit-Remaining")
        reset_str = headers.get("X-RateLimit-Reset")
        remaining = int(remaining_str) if remaining_str and remaining_str.isdigit() else None
        reset_epoch = int(reset_str) if reset_str and reset_str.isdigit() else None

        return RateLimitInfo(remaining=remaining, reset_epoch=reset_epoch)


    def maybe_sleep_for_rate_limit(self, headers: Dict[str, str], minimum_remaining: int = 2) -> None:
        info = self.parse_rate_limit(headers)
        if info.remaining is not None and info.remaining <= minimum_remaining:
            # Sleep until reset with a small buffer
            reset_time = info.reset_epoch or (int(time.time()) + 60)
            sleep_seconds = max(0, reset_time - int(time.time()) + 1)
            time.sleep(sleep_seconds)

    async def maybe_sleep_for_rate_limit_async(self, headers: Dict[str, str], minimum_remaining: int = 2) -> None:
        info = self.parse_rate_limit(headers)
        if info.remaining is not None and info.remaining <= minimum_remaining:
            reset_time = info.reset_epoch or (int(time.time()) + 60)
            sleep_seconds = max(0, reset_time - int(time.time()) + 1)
            await asyncio.sleep(sleep_seconds)


    async def search_total_count(self, session: aiohttp.ClientSession, star_filter: str) -> Tuple[int, Dict[str, str]]:
        params = {
            "q": f"stars:{star_filter}",
            "per_page": "1",
        }
        url = f"{self.SEARCH_REPOS_ENDPOINT}?{urlparse.urlencode(params)}"
        data, headers = await self.async_github_request(session, url)
        total_count = int(data.get("total_count", 0))
        print("[+] Total count search ", total_count)
        return total_count, headers

    async def search_items(self, session: aiohttp.ClientSession, star_filter: str) -> AsyncIterator[dict]:
        # Paginate through up to 1,000 results for a given filter (<= 1,000 ensured by caller)
        per_page = 100
        total, headers = await self.search_total_count(session, star_filter)
        await self.maybe_sleep_for_rate_limit_async(headers)
        if total == 0:
            return

        num_pages = math.ceil(total / per_page)
        sem = asyncio.Semaphore(self.concurrency)

        async def fetch_page(page: int) -> List[dict]:
            params = {
                "q": f"stars:{star_filter}",
                "per_page": str(per_page),
                "page": str(page),
            }
            url = f"{self.SEARCH_REPOS_ENDPOINT}?{urlparse.urlencode(params)}"
            async with sem:
                data, headers = await self.async_github_request(session, url)
                await self.maybe_sleep_for_rate_limit_async(headers)
                return data.get("items", [])

        tasks = [asyncio.create_task(fetch_page(page)) for page in range(1, num_pages + 1)]
        for coro in asyncio.as_completed(tasks):
            try:
                items = await coro
            except Exception:
                continue
            for item in items:
                yield item


    async def find_global_max_stars(self, session: aiohttp.ClientSession) -> int:
        # Find the current maximum stargazer count on GitHub for calibration
        params = {
            "q": "stars:>0",
            "sort": "stars",
            "order": "desc",
            "per_page": "1",
            "page": "1",
        }
        url = f"{self.SEARCH_REPOS_ENDPOINT}?{urlparse.urlencode(params)}"
        data, headers = await self.async_github_request(session, url)
        await self.maybe_sleep_for_rate_limit_async(headers)
        items = data.get("items", [])
        if not items:
            return 1_000_000

        top = items[0]

        return int(top.get("stargazers_count", 1_000_000))


    async def binary_find_upper_bound_for_range(self, session: aiohttp.ClientSession, min_exclusive: int, global_max: int) -> int:
        """Find the largest upper bound U such that count(min_exclusive+1..U) <= 1000.

        Returns U >= min_exclusive+1. If no such U exists (unlikely), returns min_exclusive+1.
        """
        low = min_exclusive + 1
        high = global_max
        best = None
        while low <= high:
            mid = (low + high) // 2
            star_filter = f"{min_exclusive + 1}..{mid}"
            count, headers = await self.search_total_count(session, star_filter)
            await self.maybe_sleep_for_rate_limit_async(headers)
            if count == 0:
                low = mid + 1
                continue
            if count > 1000:
                high = mid - 1
            else:
                best = mid
                low = mid + 1
        if best is None:
            return min_exclusive + 1
        return best


    async def iter_all_repos_over_min_stars(self, session: aiohttp.ClientSession) -> AsyncIterator[dict]:
        """Iterate over all repositories with stargazers_count > min_stars.

        This partitions the search space by star count to avoid the 1,000 result cap.
        """
        global_max = await self.find_global_max_stars(session)
        current_min_exclusive = self.min_stars
        yielded = 0
        seen_repo_ids = set()

        while current_min_exclusive < global_max:
            upper = await self.binary_find_upper_bound_for_range(session, current_min_exclusive, global_max)
            if upper <= current_min_exclusive:
                break

            star_filter = f"{current_min_exclusive + 1}..{upper}"
            async for item in self.search_items(session, star_filter):
                repo_id = item.get("id")
                if repo_id in seen_repo_ids:
                    continue
                seen_repo_ids.add(repo_id)
                yield item
                yielded += 1
                if self.max_repos is not None and yielded >= self.max_repos:
                    return

            current_min_exclusive = upper


    def normalize_repo_record(self, item: dict) -> dict:
        """Extract a stable subset of fields for output."""
        license_info = item.get("license") or {}
        return {
            "id": item.get("id"),
            "full_name": item.get("full_name"),
            "name": item.get("name"),
            "owner_login": (item.get("owner") or {}).get("login"),
            "html_url": item.get("html_url"),
            "description": item.get("description"),
            "language": item.get("language"),
            "stargazers_count": item.get("stargazers_count"),
            "forks_count": item.get("forks_count"),
            "open_issues_count": item.get("open_issues_count"),
            "archived": item.get("archived"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
            "pushed_at": item.get("pushed_at"),
            "license_spdx": license_info.get("spdx_id"),
        }


    def write_jsonl(self, records: Iterable[dict]) -> None:
        with open(self.out_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")


    async def gen_records(self, session: aiohttp.ClientSession) -> AsyncIterator[dict]:
        async for item in self.iter_all_repos_over_min_stars(session):
            yield self.normalize_repo_record(item)


    async def generate_results_async(self):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            with open(self.out_path, "w", encoding="utf-8") as f:
                async for rec in self.gen_records(session):
                    line = json.dumps(rec, ensure_ascii=False) + "\n"
                    # Synchronous write is acceptable here; it's fast and keeps I/O simple
                    f.write(line)


    def sort_results(self):
        # Sort the output file by stargazers_count descending
        records = []
        with open(self.out_path, "r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                records.append(rec)

        records.sort(key=lambda r: r.get("stargazers_count", 0), reverse=True)

        with open(self.out_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def build_parser(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(description="Fetch GitHub repos with > N stars, overcoming the 1,000 search cap via star-range partitioning.")
    parser.add_argument("--min-stars", type=int, default=250, help="Minimum stars threshold (strictly greater than this)")
    parser.add_argument("--out", type=str, default="repos_over_250.jsonl", help="Output path")
    parser.add_argument("--format", type=str, choices=["jsonl"], default="jsonl", help="Output format")
    parser.add_argument("--token", type=str, default=os.environ.get("GITHUB_TOKEN"), help="GitHub token (env GITHUB_TOKEN used if not provided)")
    parser.add_argument("--max-repos", type=int, default=None, help="Optional limit for number of repos (for quick tests)")
    parser.add_argument("--concurrency", type=int, default=10, help="Max concurrent requests per star-range pagination")

    args = parser.parse_args(argv)

    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser(argv)

    FetchGithubRepository = FetchGithubRepos(
        token=args.token,
        max_repos=args.max_repos,
        min_stars=args.min_stars,
        out_path=args.out,
        concurrency=args.concurrency,
    )
    asyncio.run(FetchGithubRepository.generate_results_async())
    FetchGithubRepository.sort_results()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

