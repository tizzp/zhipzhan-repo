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

import time

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
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=20)
            status = resp.status_code
            if 500 <= status < 600:
                if attempt < 2:
                    time.sleep(1 + attempt)
                    continue
                print(
                    f"Warning: upstream 5xx (status {status}) for page {params.get('pageNum')}; "
                    "returning empty batch so downstream逻辑继续"
                )
                return json.dumps({"announcements": []})
            resp.raise_for_status()
            return resp.text
        except requests.HTTPError as exc:
            last_exc = exc
            status = exc.response.status_code if exc.response else None
            raise SystemExit(f"Network request failed with status {status}: {exc}") from exc
        except requests.RequestException as exc:  # capture proxy/egress issues
            last_exc = exc
            if attempt < 2:
                time.sleep(1 + attempt)
                continue
            print(
                f"Warning: network request failed after retries for page {params.get('pageNum')}: {exc}; returning empty batch"
            )
            return json.dumps({"announcements": []})
    print(f"Warning: network request failed after retries: {last_exc}; returning empty batch")
    return json.dumps({"announcements": []})


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


def classify_function(text: str) -> str:
    """Infer relocation function from text heuristics aligned to memo definitions."""
    manufacturing_keywords = [
        "生产",
        "厂",
        "产线",
        "产能",
        "技改",
        "建设",
        "退城",
        "搬迁补偿",
        "搬迁",
        "迁建",
        "迁移",
        "迁出",
        "搬出",
        "退区",
        "异地",
        "扩产",
        "新建",
        "投产",
    ]
    rd_keywords = ["研发", "实验室", "研发中心", "研发基地", "工程中心"]
    ops_keywords = ["总部", "办公", "运营", "营销", "销售", "客服", "结算", "指挥中心", "区域中心", "事业部"]

    flags = {
        "制造": any(k in text for k in manufacturing_keywords),
        "研发": any(k in text for k in rd_keywords),
        "运营": any(k in text for k in ops_keywords),
    }

    matched = [k for k, v in flags.items() if v]
    if len(matched) > 1:
        return "+".join(sorted(matched)) + "外迁"
    if flags["制造"]:
        return "制造基地外迁"
    if flags["研发"]:
        return "研发中心外迁"
    if flags["运营"]:
        return "运营中心外迁"
    return "外迁功能未披露"


def classify_destination(text: str) -> str:
    """Extract destination city/province and bucket to memo's近岸/远岸格局 when possible."""
    domestic = [
        "上海",
        "重庆",
        "成都",
        "杭州",
        "苏州",
        "南京",
        "合肥",
        "武汉",
        "长沙",
        "郑州",
        "青岛",
        "西安",
        "南昌",
        "昆明",
        "东莞",
        "惠州",
        "佛山",
        "珠海",
        "汕头",
        "揭阳",
        "潮州",
        "广西",
        "海南",
        "云南",
        "四川",
        "安徽",
        "江苏",
        "浙江",
        "江西",
        "湖北",
        "湖南",
        "河南",
        "山东",
        "广东",
        "天津",
        "北京",
    ]
    southeast_asia = ["越南", "泰国", "印尼", "马来西亚", "新加坡", "菲律宾"]
    other_overseas = ["北美", "美国", "加拿大", "欧洲", "德国", "法国", "英国"]

    for loc in domestic:
        if loc in text:
            return f"迁往{loc}（国内）"
    for loc in southeast_asia:
        if loc in text:
            return f"迁往{loc}（东南亚）"
    for loc in other_overseas:
        if loc in text:
            return f"迁往{loc}（海外）"
    return "未披露（标题未含去向）"


def classify_industry(text: str) -> str:
    """Map text to memo-aligned industry buckets."""
    mapping = [
        ("电子信息", ["半导体", "芯片", "线路板", "电子", "信息", "通信", "显示", "模组", "智能终端"]),
        ("装备制造", ["机械", "装备", "机床", "电梯", "制造", "工程机械", "机器人", "汽车", "零部件", "车身", "动力总成"]),
        ("新能源", ["光伏", "风电", "储能", "锂", "氢", "电池", "电解液", "磷酸铁锂", "负极", "正极", "逆变", "光热"]),
        ("材料", ["材料", "油墨", "化工", "涂料", "钢绳", "磷", "金属", "磁", "塑胶", "树脂", "膜"]),
        ("生物医药与医疗器械", ["医药", "药", "生物", "医疗", "诊断", "器械", "体外", "制药"]),
        ("物流与供应链", ["物流", "港口", "仓储", "供应链", "跨境", "冷链"]),
        ("信息服务", ["数据", "云", "网络", "软件", "SaaS", "AI", "人工智能"]),
        ("平台经济", ["平台", "电商", "互联网", "出行", "本地生活"]),
        ("金融与专业服务", ["金融", "基金", "证券", "保险", "律所", "咨询", "设计"]),
        ("其他服务业", ["服务", "培训", "教育", "文旅"]),
    ]
    for industry, keywords in mapping:
        if any(k in text for k in keywords):
            return industry
    # Default fallback aligned with memo buckets
    return "其他制造"


def infer_reasons(text: str) -> List[str]:
    """Infer reasons per memo's可编码维度 using keyword hints."""
    reasons: List[str] = []
    reason_keywords = [
        ("成本因素", ["成本", "租金", "人工", "能耗", "降本", "费用"]),
        ("空间与用地约束", ["腾退", "征收", "城市更新", "整备", "拆迁", "用地", "土地"]),
        ("环保与合规压力", ["环保", "排放", "整改", "超标", "碳", "EHS"]),
        ("供应链与物流", ["供应链", "配套", "零部件", "物流", "港口", "枢纽"]),
        ("市场接近", ["客户", "主机厂", "市场", "交付", "订单", "销售"]),
        ("人才与居住成本", ["人才", "研发团队", "研发人员", "校园", "招聘", "住房"]),
        ("政策激励与园区承接", ["补偿", "补助", "补贴", "激励", "奖励", "园区", "招商", "协议"]),
        ("融资与税负", ["融资", "税收", "税负", "免税", "上市", "募投"]),
        ("国际环境与地缘风险", ["关税", "出口", "贸易", "国际", "海外", "风险"]),
        ("企业战略调整", ["重组", "分拆", "并购", "剥离", "战略", "转型"]),
    ]

    for label, kws in reason_keywords:
        if any(k in text for k in kws):
            reasons.append(label)
    return reasons


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

        full_text = f"{title} {short_title}"
        function = classify_function(full_text)
        destination = classify_destination(full_text)

        reasons: List[str] = infer_reasons(full_text)

        yield EventCard(
            company=f"{company}({code})" if code else company,
            year=year,
            function=function,
            destination=destination,
            industry=classify_industry(full_text),
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
