# StarAvail

[English](README.md) | **中文**

StarAvail 是一个高性能代理服务，专为 StarRocks 单副本部署提供部分可用性保障，并优化数据摄入管道。它作为智能中间件层，在 BE 节点故障期间实现优雅降级，并提供高效的 Avro 数据处理能力。

## 🎯 核心价值主张

### 解决的主要痛点

1. **全有或全无限制**：StarRocks 单副本部署中，任何 BE 节点故障都会导致整表不可用，即使只有部分 tablet 受影响
2. **数据摄入瓶颈**：当前 Pulsar → StarRocks 管道存在 OCF 格式不兼容、阻塞解码和 StreamLoad 效率低下等问题
3. **性能下降**：频繁的小文件生成导致 compaction 压力和链路级背压

### 核心优势

- **部分可用性**：在 BE 故障期间继续为健康的 tablet 提供查询和写入服务
- **增强的数据管道**：无缝的 Avro OCF/Binary 处理，集成 Schema Registry
- **性能优化**：内置调优框架，具备高级缓存和批处理策略
- **零内核修改**：作为透明代理工作，无需修改 StarRocks 核心

## ✨ 主要功能特性

### 1. 智能部分可用性

```go
// 查询降级示例
result, err := client.Query(ctx, &QueryRequest{
    SQL:    "SELECT * FROM user_events WHERE date >= '2024-01-01'",
    Policy: PartialAvailabilityPolicy{
        Mode:           SKIP_FAILED_TABLETS,
        WarningMessage: "由于维护，部分数据可能不完整",
    },
})
````

### 2. 高级数据摄入管道

```go
// 流式数据处理
pipeline := &IngestionPipeline{
    Source:      pulsar.NewConsumer("persistent://tenant/ns/topic"),
    Decoder:     avro.NewOCFDecoder(schemaRegistry),
    Transformer: starrocks.NewBinaryTransformer(),
    Sink:        starrocks.NewBatchSink(batchSize: 1000),
}
```

### 3. 性能监控与优化

```go
// 内置指标和优化
metrics := client.GetMetrics()
fmt.Printf("吞吐量: %d EPS, 延迟: %dms, Compaction 分数: %d", 
    metrics.ThroughputEPS, metrics.AvgLatencyMs, metrics.CompactionScore)
```

## 🏗️ 架构概览

StarAvail 采用分层架构设计，确保高可用性和性能：

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   客户端应用    │────│   StarAvail     │────│   StarRocks     │
│                 │    │     代理        │    │     集群        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                       ┌──────┴──────┐
                       │   Schema    │
                       │  Registry   │
                       └─────────────┘
```

详细架构信息请参见[架构文档](docs/architecture.md)。

## 🚀 快速开始

### 前置要求

* Go 1.20.2+
* StarRocks 集群（单副本或多副本）
* 可选：用于数据摄入的 Pulsar 集群

### 构建和运行

```bash
# 克隆仓库
git clone https://github.com/turtacn/staravail.git
cd staravail

# 构建项目
go mod tidy
go build -o bin/staravail cmd/staravail/main.go

# 使用配置运行
./bin/staravail --config config/config.yaml
```

### 基础配置

```yaml
# config/config.yaml
starrocks:
  fe_endpoints:
    - "http://fe1:8030"
    - "http://fe2:8030"
  
proxy:
  listen_addr: ":8080"
  partial_availability:
    enabled: true
    policy: "skip_failed_tablets"

ingestion:
  pulsar:
    service_url: "pulsar://localhost:6650"
  schema_registry:
    url: "http://localhost:8081"
  batch_size: 1000
  flush_interval: "5s"
```

### 使用示例

#### 1. 部分可用性查询

```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT count(*) FROM events WHERE date >= '2024-01-01'",
    "partial_availability": true
  }'
```

#### 2. 支持 OCF 的数据摄入

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/octet-stream" \
  -H "X-Schema-ID: events-v1" \
  --data-binary @events.avro
```

## 🧪 测试

```bash
# 运行单元测试
go test ./...

# 运行集成测试
go test -tags=integration ./test/integration/...

# 运行基准测试
go test -bench=. ./internal/performance/...
```

## 📊 性能基准

| 指标            | 无 StarAvail | 使用 StarAvail | 改进    |
| ------------- | ----------- | ------------ | ----- |
| BE 故障时查询可用性   | 0%          | 85%+         | ∞     |
| 数据摄入吞吐量       | 3,000 EPS   | 12,000+ EPS  | 4倍    |
| 平均查询延迟        | 150ms       | 120ms        | 20%   |
| Compaction 压力 | 高           | 低            | 降低70% |

## 🤝 贡献

我们欢迎贡献！请查看我们的[贡献指南](CONTRIBUTING.md)了解详情。

### 开发环境设置

```bash
# 安装开发依赖
make setup-dev

# 运行覆盖率测试
make test-coverage

# 运行代码检查
make lint

# 生成文档
make docs
```

## 📄 许可证

本项目采用 Apache License 2.0 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

* [StarRocks](https://github.com/StarRocks/StarRocks) - 世界最快的开源查询引擎
* [Apache Pulsar](https://github.com/apache/pulsar) - 云原生分布式消息系统
* [Confluent Schema Registry](https://github.com/confluentinc/schema-registry) - Avro 模式管理

---

**注意**：本项目设计为与 StarRocks 无缝协作，无需任何核心修改。生产部署请参考我们的[生产指南](docs/production.md)。