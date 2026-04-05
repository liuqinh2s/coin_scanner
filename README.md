# 合约代币扫描器

Bitget USDT 永续合约多周期趋势共振选币工具，静态网站通过 GitHub Pages 部署。

## 架构

与 [token_scanner](https://github.com/liuqinh2s/token_scanner) 相同：
- GitHub Actions 定时任务（每 15 分钟）触发 `scripts/scan.js`
- `scripts/build.js` 将扫描数据整理到 `site/data/`，生成前端所需的静态文件
- GitHub Pages 自动部署 `site/` 目录

## 选币策略

所有条件作为独立标签，每个标签展示符合该条件的所有代币。默认组合：趋势共振 + 波动充足 + 未追高。

**标签列表（可点击切换）：**
- 趋势共振（默认）— 15m + 1H + 4H + 1D 四周期同时看多
- 成交量异动（15m/1H/4H）
- BTC 大盘方向（全局条件，不可筛选）
- 防追高（布林带宽度、7日涨幅）
- 负资金费率
- 龙头币（近5天涨幅>20%）
- 仙人指路形态
- 波动充足

## 项目结构

```
├── .github/workflows/
│   └── scan.yml              # GitHub Actions 定时任务（每 15 分钟）
├── data/                     # 扫描结果存档（按时间戳命名的 JSON）
├── scripts/
│   ├── scan.py               # 扫描 + 策略筛选 (Python + aiohttp 高并发)
│   └── build.py              # 构建静态站点数据
├── public/
│   └── index.html            # 前端页面源文件
├── site/                     # 构建产物（部署到 GitHub Pages）
│   ├── index.html
│   └── data/
│       ├── latest.json
│       ├── history.json
│       └── scans/
└── package.json
```

## 本地开发

```bash
# 安装依赖
pip3 install -r requirements.txt

# 运行扫描（结果写入 data/）
python3 scripts/scan.py

# 构建静态站点（输出到 site/）
python3 scripts/build.py

# 本地预览
python3 -m http.server 8000 -d site
```

## 部署

1. 将代码推送到 GitHub 仓库
2. 在仓库 Settings → Pages 中，Source 选择 "GitHub Actions"
3. 工作流会自动每 15 分钟运行一次扫描并部署
4. 也可在 Actions 页面手动触发 workflow_dispatch
