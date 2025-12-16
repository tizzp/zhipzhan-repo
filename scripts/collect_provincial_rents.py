#!/usr/bin/env python3
"""根据链家租房数据汇总省级平均租金。

示例：
    python scripts/collect_provincial_rents.py --pages 3
"""
from __future__ import annotations

import argparse
import csv
from typing import Dict, List

from lianjia_rent_scraper import CITY_CODES, RentRecord, compute_avg_price, fetch_city, save_to_csv

# 省份与链家城市编码的映射，可根据需要扩展。
# 这里只选择了若干示范省份及其主要城市。
PROVINCE_CITIES: Dict[str, List[str]] = {
    "北京市": ["bj"],
    "上海市": ["sh"],
    "广东省": ["gz", "sz"],
}


def collect_province(province: str, cities: List[str], pages: int) -> List[RentRecord]:
    """抓取某省所有指定城市的租金数据并汇总。"""
    records: List[RentRecord] = []
    for city in cities:
        city_name = CITY_CODES.get(city, city)
        print(f"抓取 {city_name} ({city}) ...")
        records.extend(fetch_city(city, pages))
    return records


def write_provincial_averages(data: Dict[str, List[RentRecord]], path: str) -> None:
    """将省级平均租金写入CSV。"""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["province", "avg_rent_yuan_per_sqm"])
        for province, records in data.items():
            avg = compute_avg_price(records)
            writer.writerow([province, f"{avg:.2f}"])


def main() -> None:
    parser = argparse.ArgumentParser(description="按省份汇总链家租金数据")
    parser.add_argument("--pages", type=int, default=5, help="每个城市抓取的页数")
    parser.add_argument("--raw", default="rent_raw.csv", help="保存所有房源的CSV")
    parser.add_argument("--output", default="provincial_rents.csv", help="省级平均租金CSV")
    args = parser.parse_args()

    province_records: Dict[str, List[RentRecord]] = {}
    all_records: List[RentRecord] = []

    for province, cities in PROVINCE_CITIES.items():
        records = collect_province(province, cities, args.pages)
        province_records[province] = records
        all_records.extend(records)
        avg = compute_avg_price(records)
        print(f"{province}平均租金: {avg:.2f} 元/平方米·月")

    save_to_csv(all_records, args.raw)
    write_provincial_averages(province_records, args.output)
    print(f"原始数据保存至 {args.raw}，省级平均租金保存至 {args.output}")


if __name__ == "__main__":
    main()
