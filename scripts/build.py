"""
Build Script — 生成静态 JSON 文件供 GitHub Pages 前端使用

读取 data/ 中的扫描结果，生成:
  site/data/latest.json    - 最新扫描结果
  site/data/history.json   - 历史扫描索引
  site/data/scans/N.json   - 各次扫描详情 (0 = 最新)

同时复制 public/index.html 到 site/index.html，重写 API 路径为静态文件路径
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
SCANS_DIR = SITE_DATA_DIR / "scans"
PUBLIC_DIR = ROOT / "public"

# 确保输出目录存在
for d in (SITE_DIR, SITE_DATA_DIR, SCANS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# 读取所有扫描文件，按文件名倒序（最新在前）
scan_files = sorted(DATA_DIR.glob("*.json"), reverse=True)

if not scan_files:
    print("[BUILD] 无扫描数据，写入空默认值")
    empty = {
        "scanTime": None, "totalSymbols": 0, "validSymbols": 0,
        "filteredCount": 0, "btcDirection": "unknown", "elapsed": 0,
        "tokens": [],
    }
    (SITE_DATA_DIR / "latest.json").write_text(json.dumps(empty))
    (SITE_DATA_DIR / "history.json").write_text(json.dumps([]))
else:
    # latest.json = 最新一次扫描
    latest_data = json.loads(scan_files[0].read_text())
    (SITE_DATA_DIR / "latest.json").write_text(json.dumps(latest_data))

    # history.json + 各次扫描文件
    history = []
    for idx, f in enumerate(scan_files):
        data = json.loads(f.read_text())
        history.append({
            "id": idx,
            "scan_time": data.get("scanTime"),
            "total_symbols": data.get("totalSymbols", 0),
            "filtered_count": data.get("filteredCount", 0),
        })
        (SCANS_DIR / f"{idx}.json").write_text(json.dumps(data))

    (SITE_DATA_DIR / "history.json").write_text(json.dumps(history))
    print(f"[BUILD] 生成 {len(scan_files)} 次扫描数据")

# 复制并修补 index.html — 将 API 路径重写为静态文件路径
html = (PUBLIC_DIR / "index.html").read_text()

html = re.sub(
    r"cachedFetch\('/api/latest'",
    "cachedFetch('data/latest.json'",
    html,
)
html = re.sub(
    r"cachedFetch\('/api/history'\)",
    "cachedFetch('data/history.json')",
    html,
)
html = re.sub(
    r"cachedFetch\('/api/scan/' \+ id\)",
    "cachedFetch('data/scans/' + id + '.json')",
    html,
)

(SITE_DIR / "index.html").write_text(html)
print("[BUILD] 站点构建完成 -> site/")
