# StarAvail - StarRocks 高可用代理服务

[![Go 版本](https://img.shields.io/badge/Go-1.20.2+-blue.svg)](https://golang.org)
[![许可证](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![构建状态](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](https://github.com/turtacn/staravail)

中文文档 | [English](README.md)

StarAvail 是一个智能代理服务，专门为运行在单副本模式（num_replicas=1）下的 StarRocks 集群提供"部分可用"能力。当后端（BE）节点故障时，StarAvail 通过智能路由查询和写入请求绕过故障 tablet，确保业务连续性，同时维护数据一致性。

## 核心痛点与价值主张

### 挑战描述
在 StarRocks 单副本部署中，一张用户表可能被拆分为几十上百个 tablet。任意一个 BE 节点宕机，其上全部 tablet 即变为不可用。StarRocks 的"全或无"语义导致：

- **服务中断**：即使查询只涉及健康 tablet，整表查询也会失败
- **数据丢失风险**：单副本场景下存在永久数据丢失的可能
- **业务影响**：大面积查询和写入失败，影响业务运营

### 我们的解决方案
StarAvail 作为智能代理层提供：
- **维持服务连续性**：仅向健康 tablet 路由请求
- **防止数据丢失**：智能缓存发往故障分区的写入请求并重试
- **零内核修改**：无需修改 StarRocks 核心即可工作
- **性能优化**：内置性能调优框架，具备可扩展的优化能力

## 主要功能特性

### 🚀 智能可用性管理
- 通过 FE HTTP API 实时监控 BE 节点健康状态
- 动态维护 tablet-to-BE 映射关系，智能缓存机制
- 自动将查询修剪到仅涉及健康分区

### 🛡️ 数据保护
- 对故障分区的写入进行缓冲，自动重试机制
- 可配置的降级策略（拒绝 vs. 部分结果）
- 事务感知的写入协调

### ⚡ 卓越性能
- 正常操作下的高性能反向代理
- 可扩展的性能优化框架
- 连接池和查询结果缓存
- 跨健康 FE 节点的智能负载均衡

### 🔧 运维友好
- 全面的可观测性：指标、日志和链路追踪
- 分区故障的可配置告警
- 监控集成的健康检查端点
- 优雅降级与清晰的错误消息

## 架构概览

StarAvail 作为应用程序和 StarRocks FE 节点之间的无状态代理层运行：

````

\[应用程序] → \[StarAvail 代理] → \[StarRocks FE 集群]
↓
\[配置与状态管理]
↓
\[StarRocks BE 集群]

````

详细架构文档请参阅 [docs/architecture.md](docs/architecture.md)。

## 快速开始

### 前置要求
- Go 1.20.2 或更高版本
- StarRocks 集群（单副本模式）
- StarRocks FE HTTP API 访问权限

### 安装

```bash
# 克隆仓库
git clone https://github.com/turtacn/staravail.git
cd staravail

# 构建项目
make build

# 或直接安装
go install github.com/turtacn/staravail/cmd/staravail@latest
````

### 基本用法

```bash
# 启动 StarAvail 代理
./staravail --config config.yaml

# 或使用环境变量
export STARAVAIL_FE_HOSTS="fe1:9030,fe2:9030,fe3:9030"
export STARAVAIL_LISTEN_PORT="8030"
./staravail
```

### 配置示例

```yaml
# config.yaml
server:
  listen_port: 8030
  read_timeout: 30s
  write_timeout: 30s

starrocks:
  fe_hosts:
    - "fe1:9030"
    - "fe2:9030"  
    - "fe3:9030"
  health_check_interval: 10s
  tablet_mapping_refresh: 60s

availability:
  degradation_mode: "partial_results" # 或 "reject"
  write_buffer_size: 1000
  retry_attempts: 3
  retry_backoff: "exponential"

observability:
  metrics_enabled: true
  tracing_enabled: true
  log_level: "info"
```

### 使用示例

```go
// 通过 StarAvail 代理连接，而不是直接连接 FE
db, err := sql.Open("mysql", "user:password@tcp(staravail-host:8030)/database")
if err != nil {
    log.Fatal(err)
}

// 查询自动绕过故障 tablet
rows, err := db.Query(`
    SELECT * FROM user_events 
    WHERE event_date >= '2025-01-01' 
    AND event_date < '2025-01-02'
`)

// 如果目标分区故障，写入会被智能缓冲
_, err = db.Exec(`
    INSERT INTO user_events (user_id, event_date, event_type) 
    VALUES (?, ?, ?)`, userID, eventDate, eventType)
```

## 性能优化

StarAvail 包含多项内置优化：

### 连接管理

```go
// 智能连接池
config := &PoolConfig{
    MaxIdleConns:    100,
    MaxOpenConns:    200,
    ConnMaxLifetime: time.Hour,
    HealthCheck:     true,
}
```

### 查询优化

```go
// 自动查询计划缓存
cache := NewQueryPlanCache(1000) // 缓存 1000 个计划
proxy.WithQueryPlanCache(cache)

// 重复查询的结果集缓存
resultCache := NewResultCache(time.Minute * 5)
proxy.WithResultCache(resultCache)
```

## 贡献指南

欢迎贡献！详情请参阅我们的 [贡献指南](CONTRIBUTING.md)。

### 开发环境设置

```bash
# 克隆并设置开发环境
git clone https://github.com/turtacn/staravail.git
cd staravail

# 安装开发依赖
make dev-setup

# 运行测试
make test

# 运行集成测试（需要 StarRocks 集群）
make test-integration
```

## 文档

* [架构指南](docs/architecture.md)
* [配置参考](docs/configuration.md)
* [性能调优](docs/performance.md)
* [故障排查](docs/troubleshooting.md)
* [API 参考](docs/api.md)

## 许可证

本项目采用 Apache License 2.0 许可证 - 详情请查看 [LICENSE](LICENSE) 文件。

## 社区

* GitHub Issues：[报告 bug 和功能请求](https://github.com/turtacn/staravail/issues)
* 讨论区：[加入社区讨论](https://github.com/turtacn/staravail/discussions)

---

**注意**：本项目专门为 StarRocks 单副本部署设计。对于生产环境的多副本集群，应优先使用 StarRocks 原生的可用性功能。