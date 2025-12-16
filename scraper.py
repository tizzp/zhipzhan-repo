"""
Desktop-scraping utilities for building Shenzhen enterprise relocation event cards.

The script is designed for low-dependency execution in restricted network settings while still
supporting direct online crawling when egress is available:
- Respects HTTP(S) proxy environment variables automatically via `requests`.
- Implements small, source-specific parsers with graceful degradation when pages or selectors change.
- Emits a CSV of structured "外迁事件卡片" records that can be merged into the policy memo workflow.

Usage examples
--------------
python scraper.py --source cninfo --keywords 深圳 搬迁 --limit 40 --start-date 2021-01-01 --out data/event_cards_cninfo.csv
python scraper.py --source sznews --keywords 深圳 企业 外迁 制造 --limit 30 --out data/event_cards_sznews.csv
python scraper.py --source eia --limit 50 --out data/event_cards_eia.csv
python scraper.py --source cninfo --from-html cached_cninfo.json --out data/event_cards_cninfo_offline.csv

Sources covered (extensible):
- cninfo: 巨潮资讯全文检索接口，直接联网搜索“搬迁”“迁建”等关键词的上市公司公告。
- sznews: 深圳新闻网的公开报道列表，常含企业项目动态，可用于线索收集。
- eia: 深圳生态环境局环评公示，适合识别制造基地或项目搬迁。

Note: For offline parsing, `--from-html` accepts a saved HTML/JSON body from the upstream source.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional

import pandas as pd
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
    url_builder: Optional[Callable[[Optional[List[str]]], str]] = None
    expects_json: bool = False


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def fetch_html(url: str) -> str:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    except requests.RequestException as exc:  # capture proxy/egress issues
        raise SystemExit(f"Network request failed: {exc}") from exc
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    return resp.text


def fetch_json(url: str, params: dict) -> str:
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=20)
    except requests.RequestException as exc:
        raise SystemExit(f"Network request failed: {exc}") from exc
    resp.raise_for_status()
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


def parse_cninfo(raw_json: str, keywords: Optional[List[str]] = None) -> Iterable[EventCard]:
    payload = json.loads(raw_json)
    for item in payload.get("announcements", []):
        title = item.get("announcementTitle", "")
        if keywords and not any(k in title for k in keywords):
            continue
        ts = item.get("announcementTime")
        year = dt.datetime.fromtimestamp(ts / 1000).year if ts else dt.datetime.now().year
        company = item.get("secName") or "未知"
        code = item.get("secCode") or ""
        file_path = item.get("adjunctUrl", "")
        url = f"https://static.cninfo.com.cn/{file_path}" if file_path else ""
        short_title = item.get("shortTitle") or title

        # Heuristic: if title mentions 搬迁/迁建/搬迁补偿/搬迁公告 set function to manufacturing/operations pending judgment
        function = "待判定"
        lower_title = title
        if any(k in lower_title for k in ["迁建", "搬迁", "迁移", "迁出", "腾退"]):
            function = "制造或运营外迁待判定"

        reasons: List[str] = []
        if any(k in lower_title for k in ["腾退", "征收", "城市更新", "整备"]):
            reasons.append("空间与用地约束")
        if any(k in lower_title for k in ["补偿", "补助", "补贴", "激励"]):
            reasons.append("政策激励与补偿")

        yield EventCard(
            company=f"{company}({code})" if code else company,
            year=year,
            function=function,
            destination="待判定",
            industry="待分类",
            raw_title=short_title,
            raw_summary=title,
            evidence_url=url,
            source="cninfo",
            evidence_type="上市公司公告",
            evidence_strength="强" if url else "中",
            reasons=reasons,
        )


def build_sources() -> dict:
    def sznews_url(keywords: Optional[List[str]]) -> str:
        query = " ".join(keywords) if keywords else "深圳 企业 外迁"
        return "https://search.sznews.com/search?" + requests.compat.urlencode({"keyword": query})

    def cninfo_url(_: Optional[List[str]]) -> str:
        return "https://www.cninfo.com.cn/new/fulltextSearch/full"

    return {
        "cninfo": SourceConfig(
            name="cninfo",
            url=cninfo_url(None),
            parser=parse_cninfo,
            description="巨潮资讯全文检索，捕捉上市公司搬迁/迁建公告，默认按关键词和日期过滤。",
            url_builder=cninfo_url,
            expects_json=True,
        ),
        "sznews": SourceConfig(
            name="sznews",
            url="https://search.sznews.com/search?keyword=深圳+企业+外迁",  # default keyword组合
            parser=parse_sznews,
            description="深圳新闻网资讯列表，用于捕捉企业搬迁报道。",
            url_builder=sznews_url,
        ),
        "eia": SourceConfig(
            name="eia",
            url="https://sthjj.sz.gov.cn/xxgk/xxgkml/hjgl/psj/jgxxgk/hpgs/",  # 深圳环评公示目录
            parser=parse_eia,
            description="深圳生态环境局环评公示，用于识别制造项目外迁线索。",
        ),
    }


def scrape(
    source: SourceConfig,
    keywords: Optional[List[str]],
    limit: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[EventCard]:
    target_url = source.url_builder(keywords) if source.url_builder else source.url
    results: List[EventCard] = []

    if source.expects_json:
        page = 1
        # cninfo 接口最多返回 100 条/页，此处按 page_size 控制并分页抓取直至达到 limit 或无更多记录
        while len(results) < limit:
            page_size = min(100, limit - len(results))
            params = {"pageNum": page, "pageSize": page_size}
            if keywords:
                params["searchkey"] = " ".join(keywords)
            if start_date or end_date:
                start = start_date or "2010-01-01"
                end = end_date or dt.datetime.now().strftime("%Y-%m-%d")
                params["seDate"] = f"{start}~{end}"
            raw_body = fetch_json(target_url, params=params)
            batch = list(source.parser(raw_body, keywords))
            if not batch:
                break
            results.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
    else:
        raw_body = fetch_html(target_url)
        for card in source.parser(raw_body, keywords):
            results.append(card)
            if len(results) >= limit:
                break

    return results[:limit]


def parse_from_file(source: SourceConfig, file_path: str, keywords: Optional[List[str]], limit: int) -> List[EventCard]:
    with open(file_path, "r", encoding="utf-8") as f:
        html = f.read()
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


def write_excel(cards: List[EventCard], out_path: str) -> None:
    df = pd.DataFrame([card.to_row() for card in cards])
    df.to_excel(out_path, index=False)
    print(f"Saved Excel with {len(cards)} rows to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Shenzhen enterprise relocation evidence.")
    parser.add_argument("--source", choices=["cninfo", "sznews", "eia"], required=True, help="Data source to crawl")
    parser.add_argument("--keywords", nargs="*", help="Optional Chinese keywords to filter titles")
    parser.add_argument("--limit", type=int, default=50, help="Max records to keep")
    parser.add_argument("--out", default="data/event_cards.csv", help="Output CSV path")
    parser.add_argument(
        "--xlsx-out",
        dest="xlsx_out",
        help="Optional Excel output path for users who prefer .xlsx without storing binaries in git",
    )
    parser.add_argument("--from-html", dest="from_html", help="Optional local HTML file to parse (offline)")
    parser.add_argument("--start-date", dest="start_date", help="Start date for sources supporting date filters (YYYY-MM-DD)")
    parser.add_argument("--end-date", dest="end_date", help="End date for sources supporting date filters (YYYY-MM-DD)")
    args = parser.parse_args()

    sources = build_sources()
    source = sources[args.source]
    try:
        if args.from_html:
            cards = parse_from_file(source, args.from_html, args.keywords, args.limit)
        else:
            cards = scrape(source, args.keywords, args.limit, start_date=args.start_date, end_date=args.end_date)
    except Exception as exc:  # broad log; re-raise for visibility
        raise SystemExit(f"Scrape failed for {source.name}: {exc}") from exc

    write_csv(cards, args.out)
    if args.xlsx_out:
        write_excel(cards, args.xlsx_out)
    print(f"Saved {len(cards)} rows to {args.out}")


if __name__ == "__main__":
    main()
