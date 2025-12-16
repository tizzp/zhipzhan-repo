"""
Desktop-scraping utilities for building Shenzhen enterprise relocation event cards.

The script is designed for low-dependency execution in restricted network settings:
- Respects HTTP(S) proxy environment variables automatically via `requests`.
- Implements small, source-specific parsers with graceful degradation when pages or selectors change.
- Emits a CSV of structured "外迁事件卡片" records that can be merged into the policy memo workflow.

Usage examples
--------------
python scraper.py --source sznews --keywords 深圳 企业 外迁 制造 --limit 30 --out data/event_cards_sznews.csv
python scraper.py --source eia --limit 50 --out data/event_cards_eia.csv

Sources covered (extensible):
- sznews: 深圳新闻网的公开报道列表，常含企业项目动态，可用于线索收集。
- eia: 深圳生态环境局环评公示，适合识别制造基地或项目搬迁。

Note: Network egress in this environment may be blocked; errors are captured and logged per-source.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup


@dataclass
class EventCard:
    company: str
    year: int
    function: str
    destination: str
    industry: str = ""
    origin: str = "深圳"
    evidence_url: str = ""
    evidence_type: str = "新闻/公示"
    evidence_strength: str = "中"
    reasons: List[str] = field(default_factory=list)
    raw_title: str = ""
    raw_summary: str = ""
    source: str = ""

    def to_row(self) -> dict:
        return {
            "company": self.company,
            "year": self.year,
            "function": self.function,
            "destination": self.destination,
            "industry": self.industry,
            "origin": self.origin,
            "evidence_url": self.evidence_url,
            "evidence_type": self.evidence_type,
            "evidence_strength": self.evidence_strength,
            "reasons": "|".join(self.reasons),
            "raw_title": self.raw_title,
            "raw_summary": self.raw_summary,
            "source": self.source,
        }


@dataclass
class SourceConfig:
    name: str
    url: str
    parser: Callable[[str, Optional[List[str]]], Iterable[EventCard]]
    description: str


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    return resp.text


def parse_sznews(html: str, keywords: Optional[List[str]] = None) -> Iterable[EventCard]:
    soup = BeautifulSoup(html, "html.parser")
    for li in soup.select("div.news-list li"):
        title_tag = li.find("a")
        date_tag = li.find("span", class_="date")
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        href = title_tag.get("href", "")
        # crude keyword filter to limit noise
        if keywords and not any(k in title for k in keywords):
            continue
        year = dt.datetime.now().year
        if date_tag:
            try:
                year = dt.datetime.strptime(date_tag.get_text(strip=True)[:10], "%Y-%m-%d").year
            except ValueError:
                pass
        yield EventCard(
            company="未知",
            year=year,
            function="待判定",
            destination="待判定",
            raw_title=title,
            evidence_url=href,
            source="sznews",
        )


def parse_eia(html: str, keywords: Optional[List[str]] = None) -> Iterable[EventCard]:
    soup = BeautifulSoup(html, "html.parser")
    for li in soup.select("div.list_content ul li"):
        anchor = li.find("a")
        date_tag = li.find("span")
        if not anchor:
            continue
        title = anchor.get_text(strip=True)
        if keywords and not any(k in title for k in keywords):
            continue
        href = anchor.get("href", "")
        year = dt.datetime.now().year
        if date_tag:
            try:
                year = dt.datetime.strptime(date_tag.get_text(strip=True), "%Y-%m-%d").year
            except ValueError:
                pass
        yield EventCard(
            company="待抽取",
            year=year,
            function="制造基地外迁",
            destination="待判定",
            raw_title=title,
            evidence_url=href,
            source="eia",
            evidence_type="环评公示",
            evidence_strength="强",
        )


def build_sources() -> dict:
    return {
        "sznews": SourceConfig(
            name="sznews",
            url="https://search.sznews.com/?keyword=%E5%8D%97%E5%B7%A6",  # placeholder search
            parser=parse_sznews,
            description="深圳新闻网资讯列表，用于捕捉企业搬迁报道。",
        ),
        "eia": SourceConfig(
            name="eia",
            url="https://sthjj.sz.gov.cn/xxgk/xxgkml/hjgl/psj/jgxxgk/hpgs/",  # 深圳环评公示目录
            parser=parse_eia,
            description="深圳生态环境局环评公示，用于识别制造项目外迁线索。",
        ),
    }


def scrape(source: SourceConfig, keywords: Optional[List[str]], limit: int) -> List[EventCard]:
    html = fetch_html(source.url)
    results = []
    for card in source.parser(html, keywords):
        results.append(card)
        if len(results) >= limit:
            break
    return results


def write_csv(cards: List[EventCard], out_path: str) -> None:
    if not cards:
        print(f"No data scraped; writing empty CSV to {out_path}")
    fieldnames = list(cards[0].to_row().keys()) if cards else [
        "company",
        "year",
        "function",
        "destination",
        "industry",
        "origin",
        "evidence_url",
        "evidence_type",
        "evidence_strength",
        "reasons",
        "raw_title",
        "raw_summary",
        "source",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for card in cards:
            writer.writerow(card.to_row())


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Shenzhen enterprise relocation evidence.")
    parser.add_argument("--source", choices=["sznews", "eia"], required=True, help="Data source to crawl")
    parser.add_argument("--keywords", nargs="*", help="Optional Chinese keywords to filter titles")
    parser.add_argument("--limit", type=int, default=50, help="Max records to keep")
    parser.add_argument("--out", default="data/event_cards.csv", help="Output CSV path")
    args = parser.parse_args()

    sources = build_sources()
    source = sources[args.source]
    try:
        cards = scrape(source, args.keywords, args.limit)
    except Exception as exc:  # broad log; re-raise for visibility
        raise SystemExit(f"Scrape failed for {source.name}: {exc}") from exc

    write_csv(cards, args.out)
    print(f"Saved {len(cards)} rows to {args.out}")


if __name__ == "__main__":
    main()
