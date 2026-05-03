# 项目同步规则

## bitget_bot ↔ coin_scanner 同步策略

`bitget_bot` 和 `coin_scanner` 使用的是**同一套交易策略**。

- 任何策略相关的修改（标签条件、加分项、阈值、计算公式等），必须在**两个项目同步修改**。
- 代码同步：`bitget_bot/core/` 下的策略逻辑 ↔ `coin_scanner/scripts/scan.py` 中的对应实现。
- 文档同步：修改策略后必须同步更新两个项目的 `README.md` 中相关描述。
- 配置文件同步：涉及参数配置的修改，`bitget_bot/config.yaml` 和 `coin_scanner/scripts/scan.py` 中的对应常量/参数需保持一致。
