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

字段说明与自动判定（与政策备忘录口径对齐）
------------------------------------

- `function`：根据标题中的“搬迁/迁建/产线/总部/研发/运营中心”等关键词自动判定，缺乏明确信息时标记为“外迁功能未披露”。
- `destination`：优先提取标题出现的城市/省份（国内）、东南亚或欧美国家，输出如“迁往苏州（国内）”“迁往越南（东南亚）”；未出现地名时标记为“未披露（标题未含去向）”。
- `industry`：基于标题中的产品与业务关键词映射到电子信息、装备制造、新能源、材料、生物医药与医疗器械、物流与供应链、信息服务、平台经济、金融与专业服务、其他服务业或默认“其他制造”。
- `reasons`：按备忘录的 10 个可编码原因，用关键词自动填充（成本、空间与用地、环保、供应链、市场接近、人才、政策激励、融资与税负、国际环境、战略调整），便于后续频次统计。

如需人工纠正，可在 CSV/Excel 中直接覆盖以上字段，其他字段保持不变。
