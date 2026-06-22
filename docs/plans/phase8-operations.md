# Phase 8 — 运维与部署

> **产出**: Docker Compose 环境、CI/CD pipeline、监控告警、备份策略、部署脚本
>
> **前置条件**: Phase 0–7 全部完成（或至少 Phase 0–6 核心系统可运行）
>
> **验收标准**: `docker compose up` 一键启动完整开发环境；CI pipeline 通过 lint + test + build + cross-compile；Grafana 仪表盘可观测全链路指标；`deploy.sh` 可一键部署到生产 VPS

---

## 0. 工作范围

1. Docker Compose — 开发 + 生产两套配置
2. Dockerfile — 多阶段构建 Rust 二进制
3. GitHub Actions CI — lint → test → build → release
4. 监控 — Prometheus + Grafana
5. 日志聚合策略
6. 备份与灾备
7. 安全加固
8. 一键部署脚本

---

## 1. Docker Compose 配置

### 1.1 开发环境（`docker-compose.dev.yml`）

```yaml
services:
  postgres:
    image: postgres:17-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: oxide
      POSTGRES_PASSWORD: oxide_dev
      POSTGRES_DB: oxide_arb
    volumes:
      - pg_data_dev:/var/lib/postgresql/data
      - ./scripts/init-db.sql:/docker-entrypoint-initdb.d/01-init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U oxide"]
      interval: 5s
      timeout: 3s
      retries: 5

  clickhouse:
    image: clickhouse/clickhouse-server:24.12
    ports:
      - "8123:8123"   # HTTP
      - "9000:9000"   # Native
    volumes:
      - ch_data_dev:/var/lib/clickhouse
      - ./scripts/init-clickhouse.sql:/docker-entrypoint-initdb.d/01-init.sql
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  pg_data_dev:
  ch_data_dev:
```

### 1.2 生产环境（`docker-compose.prod.yml`）

```yaml
services:
  quant-pivot:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    env_file:
      - .env.production
    depends_on:
      postgres:
        condition: service_healthy
      clickhouse:
        condition: service_started
      redis:
        condition: service_healthy
    volumes:
      - ./config:/app/config:ro
      - ./static/ui:/app/static/ui:ro
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "50m"
        max-file: "5"

  postgres:
    image: postgres:17-alpine
    ports:
      - "127.0.0.1:5432:5432"
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      POSTGRES_DB: oxide_arb
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  clickhouse:
    image: clickhouse/clickhouse-server:24.12
    ports:
      - "127.0.0.1:8123:8123"
      - "127.0.0.1:9000:9000"
    volumes:
      - ch_data:/var/lib/clickhouse
      - ./config/clickhouse-users.xml:/etc/clickhouse-server/users.d/users.xml:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    ports:
      - "127.0.0.1:6379:6379"
    command: >
      redis-server
        --requirepass ${REDIS_PASSWORD}
        --maxmemory 512mb
        --maxmemory-policy allkeys-lru
        --save 900 1
        --save 300 10
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "-a", "${REDIS_PASSWORD}", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:v3.2.1
    ports:
      - "127.0.0.1:9090:9090"
    volumes:
      - ./config/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./config/alert-rules.yml:/etc/prometheus/rules/alert-rules.yml:ro
      - prom_data:/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.retention.time=30d"
      - "--web.enable-lifecycle"
    restart: unless-stopped

  grafana:
    image: grafana/grafana:11.5.2
    ports:
      - "127.0.0.1:3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: ${GRAFANA_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}
      GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
    volumes:
      - grafana_data:/var/lib/grafana
      - ./config/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./config/grafana/dashboards:/var/lib/grafana/dashboards:ro
    depends_on:
      - prometheus
    restart: unless-stopped

volumes:
  pg_data:
  ch_data:
  redis_data:
  prom_data:
  grafana_data:
```

---

## 2. Dockerfile（多阶段构建）

```dockerfile
# ─── Stage 1: Build ───────────────────────────────────────
FROM rust:1.85-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      clang libclang-dev cmake g++ ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY src/ src/

# Build release binary
RUN cargo build --release --bin quant-pivot && \
    strip target/release/quant-pivot

# ─── Stage 2: Runtime ────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates tini && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -r -s /bin/false oxide

WORKDIR /app
COPY --from=builder /build/target/release/quant-pivot /app/quant-pivot
COPY config/ /app/config/

RUN chown -R oxide:oxide /app

USER oxide

EXPOSE 8080

ENTRYPOINT ["tini", "--"]
CMD ["/app/quant-pivot", "serve", "--config-dir", "/app/config"]
```

**构建与推送**:

```bash
# 本地构建
docker build -t quant-pivot:latest .

# 带版本标签
docker build -t quant-pivot:v0.1.0 .
```

镜像体积目标：< 100MB（Rust static binary + minimal Debian runtime）。

---

## 3. GitHub Actions CI Pipeline

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  release:
    types: [published]

env:
  CARGO_TERM_COLOR: always
  RUSTFLAGS: "-D warnings"
  MSRV: "1.85"

jobs:
  # ─── Lint & Format ────────────────────────────────────
  lint:
    name: Lint & Format
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@v2

      - name: Format check
        run: cargo fmt --all -- --check

      - name: Clippy
        run: cargo clippy --workspace --all-targets -- -D warnings

  # ─── Test ──────────────────────────────────────────────
  test:
    name: Test
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17-alpine
        env:
          POSTGRES_USER: oxide
          POSTGRES_PASSWORD: test
          POSTGRES_DB: quant_pivot_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 3s
          --health-retries 5
      clickhouse:
        image: clickhouse/clickhouse-server:24.12
        ports:
          - 8123:8123
          - 9000:9000
      redis:
        image: redis:7-alpine
        ports:
          - 6379:6379
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2

      - name: Run tests
        env:
          DATABASE_URL: postgres://oxide:test@localhost:5432/quant_pivot_test
          CLICKHOUSE_URL: http://localhost:8123
          REDIS_URL: redis://localhost:6379
        run: cargo test --workspace

  # ─── MSRV Check ───────────────────────────────────────
  msrv:
    name: MSRV Check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ env.MSRV }}
      - uses: Swatinem/rust-cache@v2
      - run: cargo check --workspace

  # ─── Build ────────────────────────────────────────────
  build:
    name: Build (${{ matrix.target }})
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        include:
          - target: x86_64-unknown-linux-gnu
            os: ubuntu-latest
            cross: true
          - target: aarch64-unknown-linux-gnu
            os: ubuntu-latest
            cross: true
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
        with:
          key: ${{ matrix.target }}

      - name: Install cross
        if: matrix.cross
        run: cargo install cross --git https://github.com/cross-rs/cross

      - name: Build
        run: |
          if [ "${{ matrix.cross }}" = "true" ]; then
            cross build --release --target ${{ matrix.target }}
          else
            cargo build --release --target ${{ matrix.target }}
          fi

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: quant-pivot-${{ matrix.target }}
          path: target/${{ matrix.target }}/release/quant-pivot
          retention-days: 30

  # ─── Benchmarks ────────────────────────────────────────
  bench:
    name: Benchmarks
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2

      - name: Run benchmarks
        run: cargo bench --workspace -- --output-format bencher | tee bench_results.txt

      - uses: actions/upload-artifact@v4
        with:
          name: bench-results-${{ github.sha }}
          path: bench_results.txt
          retention-days: 90

  # ─── Release ───────────────────────────────────────────
  release:
    name: Release
    needs: [lint, test, msrv, build]
    runs-on: ubuntu-latest
    if: github.event_name == 'release'
    steps:
      - uses: actions/download-artifact@v4
        with:
          pattern: quant-pivot-*
          path: artifacts/

      - name: Upload release assets
        uses: softprops/action-gh-release@v2
        with:
          files: |
            artifacts/quant-pivot-x86_64-unknown-linux-gnu/quant-pivot
            artifacts/quant-pivot-aarch64-unknown-linux-gnu/quant-pivot
```

### 3.1 CI 流程图

```
PR / push to main
      │
      ├── lint (fmt + clippy)         ──┐
      ├── test (unit + integration)    ──┼── 并行执行
      ├── msrv (Rust 1.85 check)       ──┤
      └── build (x86_64 + aarch64)     ──┘
                                         │
                                    all pass?
                                         │
                                    ┌────┴─────┐
                              (on release tag)  │
                                    │           │
                              release job       │
                              (upload binary)   │
                                                │
                                           (on push main)
                                                │
                                           bench job
                                           (store results)
```

---

## 4. Grafana Dashboard 设计

### 4.1 Dashboard 列表

| Dashboard | 用途 | 核心面板数 |
|---|---|---|
| **System Overview** | 系统级健康状态一览 | 8 |
| **Trading Performance** | 交易 PnL + 执行指标 | 10 |
| **Market Data Pipeline** | 数据流健康 + 延迟 | 8 |
| **Risk Engine** | 风控引擎状态 + 熔断器 | 6 |
| **Infrastructure** | DB / CH / Redis 资源使用 | 8 |

### 4.2 System Overview 面板

| 面板 | 类型 | PromQL 查询 |
|---|---|---|
| Uptime | Stat | `process_uptime_seconds` |
| Active Markets | Stat | `quant_pivot_active_markets` |
| Execution Mode | Stat | `quant_pivot_execution_mode` |
| System Status | State timeline | `quant_pivot_system_status` |
| CPU Usage | Time series | `rate(process_cpu_seconds_total[1m])` |
| Memory Usage | Time series | `process_resident_memory_bytes` |
| Open File Descriptors | Time series | `process_open_fds` |
| Goroutines / Tokio Tasks | Time series | `tokio_tasks_active` |

### 4.3 Trading Performance 面板

| 面板 | 类型 | PromQL 查询 |
|---|---|---|
| Daily PnL | Stat (green/red) | `quant_pivot_daily_pnl_usd` |
| Cumulative PnL | Time series | `quant_pivot_cumulative_pnl_usd` |
| Trade Count (24h) | Stat | `increase(quant_pivot_trades_total[24h])` |
| Win Rate (24h) | Gauge | `increase(quant_pivot_trades_total{outcome="success"}[24h]) / increase(quant_pivot_trades_total[24h])` |
| Trade PnL Distribution | Histogram | `quant_pivot_trade_pnl_usd_bucket` |
| Avg Edge (bps) | Time series | `quant_pivot_avg_edge_bps` |
| Fill Rate | Time series | `rate(quant_pivot_fills_total[5m])` |
| Execution Latency | Heatmap | `quant_pivot_execution_latency_seconds_bucket` |
| Opportunities Detected | Time series | `rate(quant_pivot_opportunities_detected_total[5m])` |
| Opportunity → Trade Conversion | Stat | `increase(quant_pivot_trades_total[24h]) / increase(quant_pivot_opportunities_detected_total[24h])` |

### 4.4 Market Data Pipeline 面板

| 面板 | 类型 | PromQL 查询 |
|---|---|---|
| WS Connection Status | State timeline | `quant_pivot_ws_connected` |
| Tick Rate | Time series | `rate(quant_pivot_ticks_received_total[1m])` |
| Book Update Latency | Heatmap | `quant_pivot_book_update_latency_seconds_bucket` |
| Stale Books | Time series | `quant_pivot_stale_books_count` |
| Gamma Sync Status | State timeline | `quant_pivot_gamma_last_sync_success` |
| Gamma Sync Duration | Time series | `quant_pivot_gamma_sync_duration_ms` |
| WS Reconnects | Counter | `increase(quant_pivot_ws_reconnects_total[1h])` |
| CH Write Latency | Time series | `quant_pivot_ch_write_latency_seconds` |

### 4.5 Risk Engine 面板

| 面板 | 类型 | PromQL 查询 |
|---|---|---|
| Circuit Breaker State | State timeline | `quant_pivot_circuit_breaker_level` |
| Daily Loss vs Limit | Gauge | `quant_pivot_daily_loss_usd / quant_pivot_daily_loss_limit_usd` |
| Open Positions | Stat | `quant_pivot_open_positions_count` |
| Total Exposure (USD) | Stat | `quant_pivot_total_exposure_usd` |
| Risk Check Latency | Heatmap | `quant_pivot_risk_check_latency_seconds_bucket` |
| Risk Denials | Time series | `rate(quant_pivot_risk_denials_total[5m])` |

---

## 5. Prometheus Alert Rules

```yaml
# config/alert-rules.yml

groups:
  - name: quant-pivot-critical
    rules:
      # 系统级告警 — 应立即处理
      - alert: SystemDown
        expr: up{job="quant-pivot"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "quant-pivot 进程不可达"
          description: "Prometheus 无法抓取 quant-pivot metrics 超过 1 分钟"

      - alert: CircuitBreakerL4
        expr: quant_pivot_circuit_breaker_level >= 4
        for: 0s
        labels:
          severity: critical
        annotations:
          summary: "L4 紧急熔断器触发"
          description: "系统已进入紧急停止状态，所有交易暂停"

      - alert: DailyLossExceeded
        expr: quant_pivot_daily_loss_usd > quant_pivot_daily_loss_limit_usd * 0.8
        for: 0s
        labels:
          severity: critical
        annotations:
          summary: "当日亏损超过限额 80%"
          description: "当日亏损 {{ $value }} USD，已接近限额"

      - alert: DatabaseDown
        expr: quant_pivot_db_healthy == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "PostgreSQL 连接断开"

      - alert: ClickHouseDown
        expr: quant_pivot_ch_healthy == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "ClickHouse 连接断开"

  - name: quant-pivot-warning
    rules:
      # 预警 — 应在合理时间内处理
      - alert: HighLatencyExecution
        expr: histogram_quantile(0.99, rate(quant_pivot_execution_latency_seconds_bucket[5m])) > 2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "交易执行延迟 P99 > 2s"

      - alert: StaleBookData
        expr: quant_pivot_stale_books_count > 5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "超过 5 个市场 book 数据过期"

      - alert: WsDisconnected
        expr: quant_pivot_ws_connected == 0
        for: 30s
        labels:
          severity: warning
        annotations:
          summary: "WebSocket 连接断开超过 30s"

      - alert: HighMemoryUsage
        expr: process_resident_memory_bytes / 1024 / 1024 > 2048
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "内存使用超过 2GB"

      - alert: CircuitBreakerL2
        expr: quant_pivot_circuit_breaker_level >= 2
        for: 0s
        labels:
          severity: warning
        annotations:
          summary: "L2 熔断器触发（当日亏损）"

      - alert: LowDiskSpace
        expr: node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"} < 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "磁盘可用空间 < 10%"
```

### 5.1 告警通知渠道

| 级别 | 通知方式 | 说明 |
|---|---|---|
| critical | Telegram + Webhook + UI alert | 需要立即人工干预 |
| warning | Telegram + UI notification | 需关注，可能恶化 |
| info | UI notification only | 信息性，无需处理 |

Alertmanager 配置通过 `config/alertmanager.yml` 管理，支持静默窗口和路由规则。

---

## 6. 日志聚合策略

### 6.1 日志输出

```rust
// Rust 应用日志配置
tracing_subscriber::fmt()
    .with_env_filter("oxide_arb=info,tower_http=debug")
    .json()  // 结构化 JSON 输出
    .with_file(true)
    .with_line_number(true)
    .with_target(true)
    .init();
```

### 6.2 日志存储策略

| 层级 | 存储 | 保留期 |
|---|---|---|
| 应用日志 (JSON) | Docker json-file driver → 本地文件 | 30 天 |
| 交易日志 | PostgreSQL `trades` + `lifecycle_events` 表 | 永久 |
| Tick 数据 | ClickHouse `tick_events` 表 | 90 天（可配置） |
| 审计日志 | PostgreSQL `runtime_config` 表 | 永久 |

### 6.3 日志轮转

```yaml
# Docker logging driver 配置
logging:
  driver: json-file
  options:
    max-size: "50m"
    max-file: "5"
```

对于需要更长期存储的场景，可通过 cron job 定期将日志压缩归档到外部存储。

---

## 7. 备份策略

### 7.1 PostgreSQL 备份

```bash
#!/bin/bash
# scripts/backup-postgres.sh

BACKUP_DIR="/backup/postgres"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETAIN_DAYS=30

mkdir -p "$BACKUP_DIR"

# Full dump (custom format for parallel restore)
docker compose exec -T postgres pg_dump \
  -U "$PG_USER" \
  -Fc \
  --no-owner \
  oxide_arb > "$BACKUP_DIR/quant_pivot_${TIMESTAMP}.dump"

# Compress
gzip "$BACKUP_DIR/quant_pivot_${TIMESTAMP}.dump"

# Cleanup old backups
find "$BACKUP_DIR" -name "*.dump.gz" -mtime +$RETAIN_DAYS -delete

echo "PostgreSQL backup completed: quant_pivot_${TIMESTAMP}.dump.gz"
```

Cron 调度：每日 UTC 04:00（交易低谷期）

```cron
0 4 * * * /opt/quant-pivot/scripts/backup-postgres.sh >> /var/log/quant-pivot-backup.log 2>&1
```

### 7.2 ClickHouse 备份

```bash
#!/bin/bash
# scripts/backup-clickhouse.sh

BACKUP_DIR="/backup/clickhouse"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="quant_pivot_${TIMESTAMP}"
RETAIN_DAYS=14

# Use ClickHouse built-in BACKUP command
docker compose exec -T clickhouse clickhouse-client --query \
  "BACKUP DATABASE default TO Disk('backups', '${BACKUP_NAME}')"

# Compress and move
tar -czf "$BACKUP_DIR/${BACKUP_NAME}.tar.gz" \
  -C /var/lib/clickhouse/backups "$BACKUP_NAME"

# Cleanup
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +$RETAIN_DAYS -delete

echo "ClickHouse backup completed: ${BACKUP_NAME}.tar.gz"
```

### 7.3 备份校验

每周自动恢复到隔离环境验证：

```bash
#!/bin/bash
# scripts/verify-backup.sh

LATEST_PG=$(ls -t /backup/postgres/*.dump.gz | head -1)
LATEST_CH=$(ls -t /backup/clickhouse/*.tar.gz | head -1)

# Spin up temporary containers
docker compose -f docker-compose.verify.yml up -d

# Restore PostgreSQL
gunzip -c "$LATEST_PG" | docker compose -f docker-compose.verify.yml \
  exec -T postgres pg_restore -U oxide -d quant_pivot_verify --no-owner

# Check table counts
TRADE_COUNT=$(docker compose -f docker-compose.verify.yml exec -T postgres \
  psql -U oxide -d quant_pivot_verify -t -c "SELECT COUNT(*) FROM trades")

echo "Backup verification: $TRADE_COUNT trades restored"

# Cleanup
docker compose -f docker-compose.verify.yml down -v
```

---

## 8. Secret 管理

### 8.1 环境变量文件

```bash
# .env.production (NOT committed to git)
# ─── Database ───
PG_USER=oxide
PG_PASSWORD=<generated-strong-password>
DATABASE_URL=postgres://oxide:<password>@postgres:5432/oxide_arb

# ─── ClickHouse ───
CLICKHOUSE_URL=http://clickhouse:8123
CLICKHOUSE_USER=oxide
CLICKHOUSE_PASSWORD=<generated-strong-password>

# ─── Redis ───
REDIS_PASSWORD=<generated-strong-password>
REDIS_URL=redis://:<password>@redis:6379

# ─── Trading Keys ───
QUANT_PIVOT__KEYS__PRIVATE_KEY_HEX=<ethereum-private-key>

# ─── API ───
QUANT_PIVOT__KEYS__API_KEY=<api-key-for-web-ui>

# ─── Notifications ───
QUANT_PIVOT__NOTIFICATION__TELEGRAM__BOT_TOKEN=<telegram-bot-token>
QUANT_PIVOT__NOTIFICATION__TELEGRAM__CHAT_ID=<telegram-chat-id>

# ─── Grafana ───
GRAFANA_PASSWORD=<grafana-admin-password>
```

### 8.2 安全规则

| 规则 | 说明 |
|---|---|
| `.env*` 在 `.gitignore` 中 | 永不提交敏感文件 |
| 密码最小长度 32 字符 | 使用 `openssl rand -base64 32` 生成 |
| 私钥运行时从环境变量加载 | 不使用文件存储明文私钥 |
| API key 每 90 天轮换 | 通过 config PATCH API + 进程重启 |
| Redis 使用密码认证 | 生产环境必须 `requirepass` |
| PostgreSQL 禁止远程连接 | `ports: 127.0.0.1:5432:5432` |
| ClickHouse 绑定本地 | `ports: 127.0.0.1:8123:8123` |

### 8.3 密钥轮换流程

```bash
# 1. 生成新密钥
NEW_KEY=$(openssl rand -base64 32)

# 2. 更新 .env.production
sed -i "s/^QUANT_PIVOT__KEYS__API_KEY=.*/QUANT_PIVOT__KEYS__API_KEY=$NEW_KEY/" .env.production

# 3. 重启服务（graceful）
docker compose -f docker-compose.prod.yml restart quant-pivot

# 4. 更新 UI 配置中的 API key
echo "New API key: $NEW_KEY"
```

---

## 9. 一键部署脚本

```bash
#!/bin/bash
# scripts/deploy.sh
set -euo pipefail

# ─── Configuration ──────────────────────────────────────
DEPLOY_DIR="/opt/quant-pivot"
REPO_URL="git@github.com:<user>/quant-pivot.git"
BRANCH="${1:-main}"

echo "=== quant-pivot deployment ==="
echo "Branch: $BRANCH"
echo "Deploy dir: $DEPLOY_DIR"

# ─── Pre-flight checks ──────────────────────────────────
command -v docker >/dev/null 2>&1 || { echo "docker not found"; exit 1; }
command -v docker compose >/dev/null 2>&1 || { echo "docker compose not found"; exit 1; }

# ─── Pull latest code ───────────────────────────────────
if [ -d "$DEPLOY_DIR/.git" ]; then
  cd "$DEPLOY_DIR"
  git fetch origin
  git checkout "$BRANCH"
  git pull origin "$BRANCH"
else
  git clone -b "$BRANCH" "$REPO_URL" "$DEPLOY_DIR"
  cd "$DEPLOY_DIR"
fi

# ─── Check .env.production exists ────────────────────────
if [ ! -f .env.production ]; then
  echo "ERROR: .env.production not found. Copy from .env.production.example and fill in secrets."
  exit 1
fi

# ─── Build UI (if source changed) ───────────────────────
if [ -d "oxide-arb-ui" ]; then
  echo "Building UI..."
  cd oxide-arb-ui
  pnpm install --frozen-lockfile
  pnpm build
  cp -r dist/ ../static/ui/
  cd ..
fi

# ─── Build and deploy ───────────────────────────────────
echo "Building Docker images..."
docker compose -f docker-compose.prod.yml build

echo "Running database migrations..."
docker compose -f docker-compose.prod.yml run --rm quant-pivot \
  /app/quant-pivot migrate --config-dir /app/config

echo "Starting services..."
docker compose -f docker-compose.prod.yml up -d

# ─── Health check ────────────────────────────────────────
echo "Waiting for health check..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
    echo "✓ quant-pivot is healthy"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Health check failed after 30 seconds"
    docker compose -f docker-compose.prod.yml logs --tail 50 quant-pivot
    exit 1
  fi
  sleep 1
done

echo "=== Deployment complete ==="
docker compose -f docker-compose.prod.yml ps
```

---

## 10. Health Check 与自动重启

### 10.1 systemd Service（备选方案，如不用 Docker）

```ini
# /etc/systemd/system/quant-pivot.service

[Unit]
Description=quant-pivot prediction market arbitrage
After=network-online.target postgresql.service clickhouse-server.service redis.service
Wants=network-online.target

[Service]
Type=simple
User=oxide
Group=oxide
WorkingDirectory=/opt/quant-pivot
EnvironmentFile=/opt/quant-pivot/.env.production
ExecStart=/opt/quant-pivot/quant-pivot serve --config-dir /opt/quant-pivot/config
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=5
StartLimitBurst=5
StartLimitIntervalSec=60

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/opt/quant-pivot/data /opt/quant-pivot/logs

# Resource limits
LimitNOFILE=65535
MemoryMax=4G

[Install]
WantedBy=multi-user.target
```

### 10.2 Docker 健康检查

```yaml
# docker-compose.prod.yml 中已配置
healthcheck:
  test: ["CMD", "curl", "-sf", "http://localhost:8080/health"]
  interval: 15s
  timeout: 5s
  retries: 3
  start_period: 30s
```

Docker `restart: unless-stopped` 确保容器异常退出后自动重启。

### 10.3 外部探活脚本

```bash
#!/bin/bash
# scripts/watchdog.sh — cron 每分钟执行

HEALTH_URL="http://localhost:8080/health"
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID}"

if ! curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
  TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
  MSG="⚠️ quant-pivot health check failed at $TIMESTAMP. Attempting restart..."

  # Send Telegram alert
  curl -sf "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d "chat_id=${TELEGRAM_CHAT_ID}" \
    -d "text=${MSG}" > /dev/null 2>&1

  # Restart
  cd /opt/quant-pivot
  docker compose -f docker-compose.prod.yml restart quant-pivot

  # Wait and re-check
  sleep 15
  if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
    curl -sf "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
      -d "chat_id=${TELEGRAM_CHAT_ID}" \
      -d "text=✅ quant-pivot recovered after restart" > /dev/null 2>&1
  else
    curl -sf "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
      -d "chat_id=${TELEGRAM_CHAT_ID}" \
      -d "text=🔴 quant-pivot STILL DOWN after restart. Manual intervention required." > /dev/null 2>&1
  fi
fi
```

Cron 调度：

```cron
* * * * * /opt/quant-pivot/scripts/watchdog.sh >> /var/log/quant-pivot-watchdog.log 2>&1
```

---

## 11. 灾难恢复 Playbook

### 11.1 场景分类

| 场景 | RTO | RPO | 恢复步骤 |
|---|---|---|---|
| 应用进程崩溃 | < 1 min | 0 | Docker 自动重启 |
| VPS 重启 | < 5 min | 0 | Docker `restart: unless-stopped` |
| PostgreSQL 数据损坏 | < 30 min | ≤ 24h | 从最近备份恢复 |
| ClickHouse 数据丢失 | < 1 hour | ≤ 24h | 从备份恢复 |
| VPS 完全损毁 | < 2 hours | ≤ 24h | 新 VPS + 备份恢复 |
| 私钥泄露 | 立即 | N/A | 紧急停止 + 转移资金 |

### 11.2 完整灾难恢复流程

```
1. 评估损失范围
   └─ 检查哪些组件受影响（app / PG / CH / Redis）

2. 紧急止损（如果交易相关）
   └─ 通过 Telegram bot 发送 /halt 命令
   └─ 或直接 POST /api/v1/system/halt

3. 准备新环境（如 VPS 损毁）
   └─ 配置新 VPS（Ubuntu 24.04, Docker, Docker Compose）
   └─ 恢复 .env.production 和 config/ 文件
   └─ git clone 代码仓库

4. 恢复数据
   └─ PostgreSQL: pg_restore 最近备份
   └─ ClickHouse: RESTORE 最近备份
   └─ Redis: 无需恢复（缓存层，自动重建）

5. 启动服务
   └─ docker compose -f docker-compose.prod.yml up -d
   └─ 执行数据库 migration（如有新版本）

6. 验证恢复
   └─ GET /health → 200
   └─ GET /ready → all checks pass
   └─ 检查最近交易记录是否完整
   └─ 检查持仓数据是否正确

7. 恢复交易
   └─ POST /api/v1/system/resume（确认一切正常后）

8. 事后复盘
   └─ 记录故障时间线
   └─ 评估数据丢失范围
   └─ 更新恢复流程文档
```

### 11.3 私钥泄露应急流程

```
1. 立即停止系统
   └─ POST /api/v1/system/halt { "reason": "key compromise" }

2. 撤销所有 pending 订单
   └─ CLOB API: cancel all open orders

3. 评估链上资产
   └─ 检查 USDC.e 余额
   └─ 检查 CTF token 持仓
   └─ 检查是否有未授权交易

4. 转移资产（如未被转走）
   └─ 使用受损密钥将资产转至安全地址
   └─ 或通过 CTF Exchange 赎回 tokens

5. 生成新密钥
   └─ 新建 Ethereum 密钥对
   └─ 更新 .env.production
   └─ 在 Polymarket 注册新 API credentials

6. 调查泄露原因
   └─ 检查服务器访问日志
   └─ 检查 .env 文件权限
   └─ 检查 git history 是否意外提交密钥
```

---

## 12. 验收检查清单

- [ ] `docker compose -f docker-compose.dev.yml up` 一键启动开发环境（PG + CH + Redis）
- [ ] `docker compose -f docker-compose.prod.yml up` 启动完整生产环境
- [ ] Dockerfile 多阶段构建成功，镜像体积 < 100MB
- [ ] CI pipeline lint → test → msrv → build 全部通过
- [ ] CI 集成测试使用 services 容器（PG + CH + Redis）
- [ ] Cross-compile 成功构建 x86_64 + aarch64 二进制
- [ ] Release tag 触发自动发布 + artifact 上传
- [ ] Prometheus 可抓取 `/metrics` 端点
- [ ] Grafana 5 个 Dashboard 正确显示指标
- [ ] Critical 告警规则在模拟故障时正确触发
- [ ] PostgreSQL 备份脚本成功执行 + 恢复验证通过
- [ ] ClickHouse 备份脚本成功执行
- [ ] `.env.production` 在 `.gitignore` 中
- [ ] 所有生产数据库端口仅绑定 `127.0.0.1`
- [ ] `deploy.sh` 可在全新 VPS 上完成首次部署
- [ ] Health check 失败后 watchdog 自动重启并发送告警
- [ ] 灾难恢复 playbook 已在隔离环境验证
- [ ] systemd service 可作为 Docker 的备选方案使用

---

## 13. 预估工作量

| 组件 | 文件 / 脚本数 | 估计工时 |
|---|---|---|
| Docker Compose (dev + prod) | 2 | 4h |
| Dockerfile | 1 | 2h |
| GitHub Actions CI | 1 | 4h |
| Prometheus config + alert rules | 2 | 3h |
| Grafana dashboards (5 个 JSON) | 5 | 8h |
| 备份脚本 (PG + CH + verify) | 3 | 3h |
| 部署脚本 + watchdog | 2 | 3h |
| systemd service | 1 | 1h |
| 文档 (playbook + runbook) | 2 | 3h |
| **合计** | **19** | **~31h** |
