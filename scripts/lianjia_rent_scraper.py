#!/usr/bin/env python3
"""抓取贝壳找房（链家）各城市租金信息并计算平均租金。

使用示例：
    python scripts/lianjia_rent_scraper.py bj sh sz

该脚本仅使用公开页面，请遵守目标网站的 robots.txt 与使用条款。
"""
import argparse
import csv
import re
import time
from dataclasses import dataclass
from typing import List

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

# 链家城市编码，如有需要可补充
CITY_CODES = {
    "bj": "北京",
    "sh": "上海",
    "gz": "广州",
    "sz": "深圳",
}

@dataclass
class RentRecord:
    city: str
    title: str
    area: float
    price: float

    @property
    def price_per_sqm(self) -> float:
        return self.price / self.area if self.area > 0 else 0.0


def parse_area(text: str) -> float:
    match = re.search(r"(\d+(?:\.\d+)?)\s*㎡", text)
    return float(match.group(1)) if match else 0.0


def fetch_city_page(city: str, page: int) -> List[RentRecord]:
    url = f"https://{city}.lianjia.com/zufang/pg{page}/"
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    records: List[RentRecord] = []
    for item in soup.select(".content__list--item"):
        title_el = item.select_one(".content__list--item--title a")
        price_el = item.select_one(".content__list--item-price em")
        des_el = item.select_one(".content__list--item--des")
        if not (title_el and price_el and des_el):
            continue
        area = parse_area(des_el.get_text())
        price = float(price_el.get_text())
        records.append(RentRecord(city=CITY_CODES.get(city, city),
                                  title=title_el.get_text(strip=True),
                                  area=area,
                                  price=price))
    return records


def fetch_city(city: str, pages: int) -> List[RentRecord]:
    all_records: List[RentRecord] = []
    for p in range(1, pages + 1):
        try:
            all_records.extend(fetch_city_page(city, p))
            time.sleep(1)
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to fetch {city} page {p}: {exc}")
            break
    return all_records


def save_to_csv(records: List[RentRecord], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["city", "title", "area_sqm", "price_yuan", "price_per_sqm"])
        for r in records:
            writer.writerow([r.city, r.title, f"{r.area:.2f}", int(r.price), f"{r.price_per_sqm:.2f}"])


def compute_avg_price(records: List[RentRecord]) -> float:
    return sum(r.price_per_sqm for r in records) / len(records) if records else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="抓取链家租金数据")
    parser.add_argument("cities", nargs="+", help="链家城市编码，如 bj sh sz")
    parser.add_argument("--pages", type=int, default=5, help="每个城市抓取的页数")
    parser.add_argument("--output", default="rent_data.csv", help="输出CSV文件")
    args = parser.parse_args()

    all_records: List[RentRecord] = []
    for city in args.cities:
        records = fetch_city(city, args.pages)
        avg_price = compute_avg_price(records)
        print(f"{CITY_CODES.get(city, city)}平均租金: {avg_price:.2f} 元/平方米·月")
        all_records.extend(records)

    save_to_csv(all_records, args.output)
    print(f"数据已保存至 {args.output}")


if __name__ == "__main__":
    main()

