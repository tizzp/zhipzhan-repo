# zhipzhan-repo

用于研究中国GDP统计中自有住房虚拟租金（OOH）改革的资料与脚本。

## 文件结构

- `docs/ooh_estimation_methods.md`：各省OOH增加值及其占GDP比重的估算数据方案。
- `scripts/lianjia_rent_scraper.py`：从链家网抓取租金数据并计算平均租金的脚本。

## 使用说明

1. 准备好各省住宅存量与GDP等基础数据。
2. 使用 `scripts/lianjia_rent_scraper.py` 抓取主要城市的租金水平：
   ```bash
   python scripts/lianjia_rent_scraper.py bj sh sz
   ```
3. 根据 `docs/ooh_estimation_methods.md` 中的步骤，将租金数据与住房存量结合，计算省级OOH及其占比。

