# Yumi 资讯采集与采编

Yumi 将公开资讯整理为可追溯的中文稿件和素材。仅采集时可本地运行。需要 AI 编稿和素材投递时，连接 DeepSeek API 和客户自己的 DataFoundation DataHub。

## 现有功能

| 环节 | 能力 |
| --- | --- |
| 候选采集 | 通过 TrendRadar 读取 11 个热榜和 3 个 RSS 源，并从已适配的官方站点和 GitHub Trending 发现选题 |
| 正文与图片 | 核实原站发布日期，提取正文与相关图片，保留出处、文件摘要和抓取记录 |
| 分类与编稿 | 覆盖 AI 综合、前沿模型、GitHub 热门项目、游戏和硬件。当前日目标每类 5 篇，编稿模型 `deepseek-flash`，每日模型调用预算 30 次。实际产量受合格来源和编稿结果影响 |
| 素材投递 | 生成 Markdown 和结构化供稿清单，写入客户数据集。上传后从 DataHub 下载原件回读，校验内容哈希 |
| 查询与恢复 | 用 SQLite 记录采集、模型调用和投递状态。只读 API 支持稿件增量分页和图片读取。服务中断后可沿原台账继续处理 |

热榜用于发现线索，完整正文覆盖已适配的公开站点。基础仓库提供采集、编稿和投递。总结图生成、数字人制作和直播发布由下游系统另行部署与验收。

## 部署资源

| 场景 | 建议配置 | 依赖 |
| --- | --- | --- |
| 采集试点 | 2 vCPU、4 GB 内存、50 GB SSD | Linux、Python 3.12、Git、uv、可访问公开来源的网络 |
| 基础采编正式运行 | 4 vCPU、8 GB 内存、100 GB SSD | 同上，另接客户的 DataHub 与 DeepSeek API。采编节点无需 GPU |
| 客户还要独立部署素材平台 | 整体可按 8 vCPU、16 GB 内存、200 GB SSD 作为低并发起步预算 | 素材平台、数据库及文件存储单独规划。本仓库提供对接程序 |

以上按单客户每天约 25 篇、低并发估算。每天 75 至 125 张图、每张 1 至 4 MB 时，图片约增加 75 至 500 MB/日。长期保存建议预留可扩容的素材空间，版本、副本和备份单独计入。模型调用按客户账号实际用量计费。对外开放只读 API 时准备 HTTPS 域名和反向代理。

## 1 安装

以下命令在 Linux 的 Bash 中执行。建议使用专用运行账号，后续手工执行和定时任务使用同一账号及同一状态目录。先通过发行版或官方渠道安装 Git、uv 和 curl。

```bash
git clone https://github.com/souleyez/yumi-news-collector.git
cd yumi-news-collector
repo="$PWD"

git clone https://github.com/sansan0/TrendRadar.git trendradar-upstream
git -C trendradar-upstream checkout --detach 68c8916c450f8c8dd36cb88a189f7c2e46681ab5
uv sync --project trendradar-upstream --frozen --python 3.12
python="$repo/trendradar-upstream/.venv/bin/python"
uv pip install --python "$python" -r requirements.txt

umask 077
mkdir -p "$repo/state/collector" "$repo/state/pipeline" "$repo/state/api"
cp -n .env.example .env
chmod 600 .env
```

`config/trendradar.yaml` 配置热榜和 RSS，`config/frequency_words.txt` 配置关键词，`config/categories.json` 配置分类规则。客户的稿件、原图、运行日志和 SQLite 状态应放在独立的持久磁盘或挂载卷中。以下示例统一使用 `state/`。

## 2 先跑采集试点

这一步使用公开来源，客户可先检查网络、原站访问和正文解析，再申请业务 Token。

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
  --collector "$repo/state/collector" --state "$repo/state/pipeline" \
  --limit 1 --collect-only
```

先看 `state/collector/latest-candidates.json` 和 `state/pipeline/health-media.json`。确认有候选、正文解析通过，并能区分无新内容、原站拒绝访问和解析失败。`--collect-only` 不调用编稿模型或写入 DataHub。

## 3 配置客户账号和 Token

1. 用获邀邮箱登录客户自己的 DataFoundation，确认目标数据集 ID，并从客户环境的 `/agent-management` 获取其官方 Agent CLI 和凭据使用指引。
2. 为采编进程准备有目标数据集上传、查询和原件回读权限的 **DataHub Agent Token**。给下游读者单独授权，只读 API 根据调用者自身的 Token 验证数据集权限。
3. 在客户 DeepSeek 开放平台准备可调用 `deepseek-flash` 的 **DeepSeek API Key**。这枚 Key 只用于编稿。

编辑上一步创建的 `.env`。以下是示例值，请换成客户自己的环境。

```dotenv
YUMI_DATAHUB_ORIGIN=https://datahub.example.com
YUMI_AI_DATASET=dataset-REPLACE_WITH_CUSTOMER_ID
YUMI_ENTERTAINMENT_DATASET=
YUMI_EXPECTED_HOST=
YUMI_NEWS_PUBLIC_BASE=
```

`YUMI_DATAHUB_ORIGIN` 只填 HTTPS 站点源地址，程序会拼接 `/api/datafoundation/agent/v1`。`YUMI_ENTERTAINMENT_DATASET` 仅为独立娱乐类别配置。`YUMI_EXPECTED_HOST` 可填目标机器的 `hostname` 作为启动检查。对外提供图片地址时，将 `YUMI_NEWS_PUBLIC_BASE` 填为只读 API 的 HTTPS 域名。

下面用当前部署账号创建权限受限的凭据文件。输入时终端不会回显。自动部署也可以由服务管理器将同名凭据通过 `LoadCredential=` 注入。

```bash
cred_dir="$HOME/.config/yumi-news/credentials"
install -d -m 700 "$cred_dir"
umask 077
read -r -s -p 'DataHub Agent Token: ' secret && [ -n "$secret" ] &&
  printf '%s' "$secret" > "$cred_dir/datahub-token"
printf '\n'
unset secret
read -r -s -p 'DeepSeek API Key: ' secret && [ -n "$secret" ] &&
  printf '%s' "$secret" > "$cred_dir/deepseek-token"
printf '\n'
unset secret
chmod 600 "$cred_dir/datahub-token" "$cred_dir/deepseek-token"
```

`CREDENTIALS_DIRECTORY` 指向凭据目录，文件名必须是 `datahub-token` 和 `deepseek-token`。`.env` 只存部署参数，Token 留在凭据目录或客户的密钥管理系统。CLI 自动化使用官方客户端支持的私有文件或标准输入方式传入 Token。

## 4 验证编稿与投递

```bash
set -a
. "$repo/.env"
set +a
export CREDENTIALS_DIRECTORY="$HOME/.config/yumi-news/credentials"
"$python" "$repo/pipeline/media_pipeline.py" \
  --collector "$repo/state/collector" --state "$repo/state/pipeline" --limit 1
```

用测试数据集确认至少一篇带来源的稿件出现在素材库，并检查 `state/pipeline/category-production-supply-upload-receipt.json` 的 `originalBytesReadbackVerified`。区分 `health-media.json` 的模型和上传阻点，以及 `daily-supply-status.json` 的分类缺额。供稿投递成功表示素材已入库，成片与上线由下游回执另行确认。

## 5 部署只读接口 可选

在上一步加载过 `.env` 的会话启动服务。将同样的命令配置为客户的常驻服务时，也要加载 `.env`。

```bash
"$python" "$repo/api/news_read_api.py" --bind 127.0.0.1 --port 18692 \
  --source "$repo/state/pipeline/category-production-supply.json" \
  --index "$repo/state/api/index.sqlite3"
```

另开终端，先执行 `curl --fail http://127.0.0.1:18692/healthz` 检查进程状态。再用有读取权限的 Token 查一页。下面从文件读取 Token，通过标准输入传给 curl。

```bash
printf 'header = "Authorization: Bearer %s"\n' "$(cat "$HOME/.config/yumi-news/credentials/datahub-token")" |
  curl --fail --silent --show-error --config - 'http://127.0.0.1:18692/api/v1/news?pageSize=1'
```

| 接口 | 说明 |
| --- | --- |
| `GET /healthz` | 进程健康检查 |
| `GET /api/v1/news` | 传 `pageSize`（1–100）。如 `hasMore` 为真，下一页传本页最后一项的 `revision` 和 `newsId` 为 `afterValue` 和 `afterId` |
| `GET /api/v1/media/<sha256>` | 根据返回的 `readPath` 获取已登记图片，服务端校验原件哈希 |

客户通过 TLS 反向代理发布上述路由，并将原 `Authorization` 头透传给本地监听。Agent 对接可以直接用标准 HTTPS Bearer 请求读取稿件和图片。

## 6 持续运行与维护

手工验收通过后，在服务器的现有调度器中配置两条任务。两条任务复用同一运行账号和状态目录，命令和环境参数分别沿用第 2 步的 `collector.py` 调用和第 4 步的 `media_pipeline.py` 调用。定时采集时把工作目录设到 `state/collector`，以便按预期写入 `output/`。采编任务加载 `.env` 并注入凭据。可先试用每两小时采集一次、每小时编稿两次，错开两轮的启动时点，并根据有效候选和模型额度调整。

- 备份 `state/`，尤其是 `delivery.sqlite3`、原稿和素材文件。备份 SQLite 时应在任务结束后做一致性快照，或使用 SQLite 在线备份机制。
- `401` 先核对 Token 有效性。`403` 核对 Token 对目标数据集的访问权限。模型调用或投递结果不明时，先查原台账和请求回执再决定是否重试。
- 升级前备份状态，并在测试数据集复验一次采集、编稿、原件回读和只读分页。异地恢复需要同时还原台账和对应文件。

## 上游组件

[TrendRadar](https://github.com/sansan0/TrendRadar) 固定为 6.11.0 的 `68c8916` 提交，由上游按 GPL-3.0 提供。安装时从上游单独获取源码、依赖锁和许可证文件。
