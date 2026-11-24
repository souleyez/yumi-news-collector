# Yumi 资讯采集与采编

面向公开资讯的定时采集、来源核验、AI 辅助编稿和素材投递。默认业务覆盖 AI 综合、前沿模型、GitHub 热门项目、游戏和硬件。

| 目录 | 用途 |
| --- | --- |
| `collector/` | 调用 TrendRadar 采集热榜和 RSS，把缓存整理成候选及分类快照 |
| `pipeline/` | 定向发现原站文章、提取正文与图片、编稿、去重、投递及回执核对 |
| `api/` | 读取已发布的供稿快照，提供分页和增量接口 |
| `config/` | 脱敏的采集源、关键词及分类规则 |

当前采集配置包含 11 个热榜来源和 3 个 RSS 来源（Hacker News、GitHub Blog 和 Hugging Face Blog）。热榜用于发现线索，正文采集覆盖已适配的公开来源。采集和编稿使用 CPU。编稿通过 DeepSeek API 执行，投递使用 DataFoundation DataHub Agent API。生图和数字人制作是独立系统。

## Linux 与 Python 3.12

使用独立的 Linux 运行目录。TrendRadar 按现网固定版本检出。以下从本仓库根目录执行，需要 Git 和 uv。

```bash
repo="$PWD"
git clone https://github.com/sansan0/TrendRadar.git trendradar-upstream
git -C trendradar-upstream checkout --detach 68c8916c450f8c8dd36cb88a189f7c2e46681ab5
uv sync --project trendradar-upstream --frozen --python 3.12
python="$repo/trendradar-upstream/.venv/bin/python"
uv pip install --python "$python" -r requirements.txt
mkdir -p "$repo/state/collector" "$repo/state/pipeline" "$repo/state/api"
```

### 采集与正文提取

```bash
export CONFIG_PATH="$repo/config/trendradar.yaml"
export FREQUENCY_WORDS_PATH="$repo/config/frequency_words.txt"
export YUMI_CATEGORIES_PATH="$repo/config/categories.json"
export TZ=Asia/Singapore
export DOCKER_CONTAINER=true
(
  cd "$repo/state/collector"
  "$python" "$repo/collector/collector.py" --state "$PWD"
)
"$python" "$repo/pipeline/media_pipeline.py" \
  --collector "$repo/state/collector" --state "$repo/state/pipeline" --collect-only
```

采集轮次会访问已配置的公开来源。`--collect-only` 执行正文和图片提取，结果保存在本地状态目录。候选缓存、SQLite 台账、原稿和图片均保存在该环境自己的运行目录。

### 编稿与素材库

按 `.env.example` 配置客户自己的 HTTPS 服务域名及 AI 新闻数据集 ID。娱乐分类可独立绑定数据集。将 `datahub-token` 和 `deepseek-token` 由运行环境管理，例如通过 systemd `LoadCredential` 注入。程序从 `CREDENTIALS_DIRECTORY` 下读取对应文件。配置好后运行同一条流水线命令并去掉 `--collect-only`。生产环境可分别为采集和采编配置 systemd 定时器，运行前应验证网络、数据集权限、模型预算、备份和失败恢复。

### 只读接口

```bash
"$python" api/news_read_api.py --bind 127.0.0.1 --port 18692 \
  --source "$repo/state/pipeline/category-production-supply.json" \
  --index "$repo/state/api/index.sqlite3"
```

新闻和图片接口使用客户 DataHub 的读取权限。对外接入通过 HTTPS 反向代理。`YUMI_NEWS_PUBLIC_BASE` 用于生成对外图片地址。只有配置 `YUMI_LIVE_ORIGIN` 才会读取自有直播页的上线证据。

## 上游与交付

[TrendRadar](https://github.com/sansan0/TrendRadar) 固定为 6.11.0 对应的 `68c8916` 提交，由上游按 GPL-3.0 提供。安装会独立获取上游源码、依赖锁和许可文件。

此版本用于版本管理和独立部署准备。客户环境须使用自己的账号、域名、数据集及持久存储，完成采集、编稿、投递和只读接入验收。
