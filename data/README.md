# 深圳企业外迁事件卡片数据

本目录包含基于公开信息生成的真实事件卡片样本：

- `event_cards_cninfo_full.csv`：使用分页模式直接联网抓取 2021–2025 年“搬迁”关键词公告得到的 320 条记录，命令示例：

  ```bash
  python scraper.py --source cninfo --keywords 搬迁 --limit 320 --start-date 2021-01-01 --end-date 2025-12-31 --out data/event_cards_cninfo_full.csv
  ```

  支持按需调整关键词与日期区间，结果字段与事件卡片模板一致，可直接导入 Excel。若网络受限，可使用 `--from-html` 解析本地缓存的 cninfo JSON。

  如需 Excel 文件而不在仓库中提交二进制，可使用新增的导出参数直接生成：

  ```bash
  python scraper.py --source cninfo --keywords 搬迁 --limit 320 --start-date 2021-01-01 --end-date 2025-12-31 --out data/event_cards_cninfo_full.csv --xlsx-out data/event_cards_cninfo_full.xlsx
  ```

- `event_cards_cninfo.csv`：早期 20 条示例输出，可作为字段对照样本。
