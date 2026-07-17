# Incremental Distributed Vector Index Build (Lance-Spark Design)

- **Status**: Draft
- **Owner**: lance-spark
- **Related**: `lance-core-centroids-codebook-reader-api.md`（lance-core 侧 API 设计，待写）
- **Last updated**: 2026-06-17

## 1. 目标与非目标

### 目标
- 在 `ALTER TABLE … CREATE INDEX` 语义下提供"只对未被现有索引覆盖的 fragment 构建段"的能力，避免全量重训和重建
- **复用**已训练的 IVF centroids 与 PQ codebook（保证查询语义一致：同一索引内所有段使用同一组中心点 / 量化码本）
- 复用现有 `VectorIndexJob` 的分布式三阶段骨架：driver 准备 → executor 并行段构建 → driver 原子提交
- 与现有 `CREATE INDEX`（全量替换）并存，行为不发生破坏性变化
- 提交语义：**add-only**——保留现存同名段，仅新增覆盖未索引 fragment 的段

### 非目标
- 不在本期支持索引**结构性参数**变更（如 `num_partitions` 改值）触发的增量；这种情况必须走全量重建
- 不支持跨 column 增量（向量列、索引名一旦确定就锁定）
- 不在本期实现 `lance-core` 内部段合并（segment compaction），保留给后续 `OPTIMIZE INDEX … COMPACT` 单独 PR

## 2. 总体架构

```
新 SQL 入口  ┐
            ├──► AddIndex(mode = INCREMENTAL) ──► AddIndexExec (扩展)
现 SQL 入口  ┘                                         │
                                                        ▼
                                          IncrementalVectorIndexJob
                                          ├─ Phase 0  fragment 差分 + 已有训练参数读取
                                          ├─ Phase 1  跳过训练，broadcast 复用 centroids/codebook
                                          ├─ Phase 2  仅对 unindexed fragments 并行构段
                                          └─ Phase 3  add-only 提交
```

复用现存类，**不新增并行架构**；仅插入"差分 + 复用"两个新阶段（Phase 0），其余路径与全量复用同一份代码。

## 3. SQL 表面

### 方案 A（推荐）：在 `CREATE INDEX` 上扩展 `mode` 参数

```sql
ALTER TABLE lance.db.items CREATE INDEX vec_idx USING ivf_pq (embedding) WITH (
    mode = 'incremental'             -- 新增；默认 'replace'（保持现状）
);
```

**优点**：grammar 零改动（`mode` 仅是 `WITH` 内的 named argument），上手成本最低，与现有索引参数一脉相承。

**关键约束**：
- 当 `mode = 'incremental'` 但**不存在同名索引**时：报错 `"Index 'vec_idx' does not exist; cannot run in incremental mode. Use mode='replace' or omit the option."`，避免静默退化为全量。
- 当 `mode = 'incremental'` 且 WITH 中显式给了 `num_partitions / num_sub_vectors / num_bits / distance_type / m / ...` 等结构参数时：与现有索引比对，**不一致即拒绝**。
- `num_segments`、`sample_rate`、`max_iters` 等"训练 / 并发调参"在增量模式下被忽略并打 warn（增量不重训，参数无效）。

### 方案 B（备选）：新增独立 DDL `OPTIMIZE INDEX`

```sql
ALTER TABLE lance.db.items OPTIMIZE INDEX vec_idx;
```

需要修改 `LanceSqlExtensions.g4` 增加规则、新增 `OptimizeIndex` 逻辑计划与 Exec、AstBuilder 在 3.4 / 3.5 / 4.x 三份中各加一处。**改动面更大**，但语义更清晰，未来扩展 `COMPACT` / `RETRAIN` 等子命令时不会污染 `CREATE INDEX`。

**取舍**：方案 A 落地更快，方案 B 长期更干净。本期先做方案 A，方案 B 作为后续兼容性别名留坑（grammar 添加新规则后让其复用同一个 Exec）。

## 4. 关键设计决策

### 4.1 fragment 差分（Phase 0）

在 `AddIndexExec.run()` 进入 logical-segment commit 路径后，新增分支：

```scala
val isIncremental = args.exists(a => a.name == "mode" && a.value == "incremental")

val (targetFragmentIds, existingSegments) = if (isIncremental) {
  val existing = dataset.getIndexes.asScala.filter(_.name() == indexName).toList
  if (existing.isEmpty) {
    throw new IllegalArgumentException(
      s"Index '$indexName' does not exist; cannot run in incremental mode")
  }
  val covered = existing
    .flatMap(_.fragments().orElse(java.util.Collections.emptyList()).asScala).toSet
  val unindexed = fragmentIds.filterNot(fid => covered.contains(fid))
  (unindexed, existing)
} else {
  (fragmentIds, Nil)
}

if (isIncremental && targetFragmentIds.isEmpty) {
  log.info(s"Index '$indexName' already covers all fragments; nothing to do")
  return Seq(new GenericInternalRow(Array[Any](0L, UTF8String.fromString(indexName))))
}
```

**行为锚点**：实现位置 `lance-spark-base_2.12/src/main/scala/.../AddIndexExec.scala:62-176`，紧跟 `useLogicalSegmentCommit` 分支之前。

### 4.2 训练参数复用（核心）

向量索引段必须用相同 IVF centroids 与 PQ codebook，否则查询会跨段不一致。两条实现路线：

| 路线 | 描述 | lance-core 依赖 | 复杂度 |
|---|---|---|---|
| **R1（MVP 兜底）** | 调用 `dataset.optimizeIndices(OptimizeOptions.builder().setIndexNames([indexName]).setRetrain(false).build())` 在 driver 完成增量构建 + 提交 | 已存在，无需改 | 低；但**非分布式**，executor 不参与 |
| **R2（目标态）** | 从已有 `Index` 反序列化 centroids / codebook，broadcast 给 executor，仅对 `unindexedFragmentIds` 并行 `dataset.createIndex(IndexOptions.withFragmentIds(...))`，最后 add-only 提交 | **需 lance-core 新增 centroids / codebook 读取 API（详见 §9）** | 高；分布式 |

**分两期上线**：
- **Phase A（本期 PR）**：实现 R1，作为 driver-only 兜底入口；使 SQL 表面、参数校验、SHOW INDEXES 可观测性、回归测试全部到位。
- **Phase B（后续 PR）**：等 lance-core 暴露读取接口后，改造为 R2。

下文 §5 的详细链路以 R2 目标态为准展开（R1 把 §5 的 Phase 1 / 2 / 3 全部塞回 driver 单线程即可）。

### 4.3 add-only 提交

`AddIndexExec.commitIndexSegments()` 现有调用 `dataset.commitExistingIndexSegments(name, column, segments)`：lance-core 这条 API 只移除"与新段 fragment 范围有重叠"的旧段。

由于增量模式下新段的 `fragmentIds` ⊂ `unindexedFragmentIds`，与已有段 fragment 集合**严格不相交**，所以同一个 API 自动实现 "old + new" 的并集语义，**无需新增 lance-core 提交接口**。

需要在 `commitIndexSegments` 上加断言保护：

```scala
if (isIncremental) {
  val newCovered = segments.flatMap(_.fragments().orElse(emptyList()).asScala).toSet
  val oldCovered = existingSegments.flatMap(_.fragments().orElse(emptyList()).asScala).toSet
  require(newCovered.intersect(oldCovered).isEmpty,
    s"Incremental commit produced segments overlapping with existing ones: " +
      s"${newCovered.intersect(oldCovered)}")
}
```

防止 R2 路线下 fragment 差分出错时静默删段。

### 4.4 并发与原子性

- **Driver 端版本锁定**：Phase 0 与 Phase 3 之间，dataset 可能因为并发写入产生新 fragment。沿用现有 `Utils.openDatasetBuilder().build()` 一次打开多次复用，或在 commit 前重新打开校验：commit 通过 `commitExistingIndexSegments` 自带读版本冲突检测。Spark 端将异常包装为 `RuntimeException("Incremental index commit lost race; retry")`。
- **训练样本一致性**：R2 不再训练，无样本一致性问题；R1 由 lance-core 内部保证。
- **任务失败语义**：与现存 `VectorIndexJob` 一致——未提交段不可见、`vacuum` 会清理；commit 失败回滚到旧索引。

## 5. 详细调用链路（R2 目标态）

```
SQL: ALTER TABLE … CREATE INDEX vec_idx USING IVF_PQ (embedding) WITH (mode='incremental')
  │
  ▼
LanceSparkSqlExtensionsParser → AddIndex(args: [..., LanceNamedArgument("mode","incremental")])
  │
  ▼
AddIndexExec.run()
  │
  ├─ Phase 0  (新增, driver):
  │     1. 解析 mode='incremental'
  │     2. 加载现有同名索引 (Dataset.getIndexes filter by name)
  │     3. 校验：索引存在 + 结构参数与请求一致
  │     4. 计算 unindexedFragmentIds = allFragments − coveredFragments
  │     5. 若空 ⇒ 直接返回 (0L, indexName)
  │     6. 从现有 Index 提取 centroids 与 (PQ变体) codebook  [需 lance-core API]
  │
  ▼
new VectorIndexJob(
    fragmentIds      = unindexedFragmentIds,           ← 仅未索引集合
    plan             = inheritedPlan,                  ← 来自现有索引,不重新解析
    presetCentroids  = Some(loadedCentroids),          ← 新增字段
    presetCodebook   = Some(loadedCodebook),
    skipTraining     = true,                           ← 新增开关
    ...).runSegments()
  │
  ├─ Phase 1' (driver): 跳过 trainAndBroadcast,直接 sc.broadcast(presetCentroids/Codebook)
  │
  ├─ Phase 2 (executors, 复用现存 VectorIndexTask):
  │     IndexUtils.batchFragments(unindexedFragmentIds, num_segments, parallelism)
  │     每个 task: createIndex(IndexOptions
  │                              .withFragmentIds(batch)
  │                              .replace(true)
  │                              .withIndexName(name))
  │       ← 关键: replace=true 让 createIndex 不抛"already exists",
  │              withFragmentIds(batch) 仅与新 fragment 相关,
  │              段产出后由 driver 提交决定是否真替换
  │
  └─ Phase 3 (driver): commitIndexSegments
        前置断言: newCovered ∩ oldCovered == ∅
        dataset.commitExistingIndexSegments(name, column, newSegments)
          ← lance-core 自动:
              · old 段 fragmentIds 与 new 段 fragmentIds 不相交 ⇒ 不删 old
              · 仅 add new
              · 在同一 manifest 事务内提交
        返回 (unindexedFragmentIds.size, indexName)
```

**关键点**：
- `VectorIndexJob` 增加可空字段 `presetCentroids: Option[Array[Float]]`、`presetCodebook: Option[Array[Float]]`、`skipTraining: Boolean`（默认 false 维持现状）。
- `trainAndBroadcast` 内 `if (skipTraining)` 直接 `(sc.broadcast(presetCentroids.get), sc.broadcast(presetCodebook.getOrElse(Array.empty)))`。
- `VectorIndexTask` **不变**——它本来就只接 broadcast，上游来源对它透明。

## 6. 模块改动清单

| 文件 | 改动 | 说明 |
|---|---|---|
| `lance-spark-base_2.12/.../AddIndexExec.scala` | 新增 Phase 0 分支、参数白名单、commit 前断言 | 主流程入口 |
| `lance-spark-base_2.12/.../VectorIndexJob.scala` | `runSegments` 接受 `skipTraining` + `presetCentroids/Codebook`，`trainAndBroadcast` 走分支 | 核心 |
| `lance-spark-base_2.12/.../VectorIndexParamsResolver.scala` | 新增 `validateIncrementalCompatibility(existingIndex, plan)`：对比 `num_partitions / num_sub_vectors / num_bits / distance_type / m / ef_construction / max_level` | 防参数漂移 |
| `lance-spark-base_2.12/.../VectorIndexPlan.scala` | 可选：`copyWithPreset(centroids, codebook)` 工厂 | 整洁 |
| `LanceSqlExtensions.g4`（仅方案 B 需要） | 新增 `OPTIMIZE INDEX` 规则 | 方案 A 不动 |
| `IndexUtils` object（在 `AddIndexExec.scala` 内） | 新增 `incrementalAllowedKeys` 与 `extractTrainingArtifacts(existingIndex)` 辅助 | 便于单测 |
| `lance-spark-base_2.12/src/test/.../BaseIncrementalAddVectorIndexTest.java` | 新增基类测试套（参考现有 `BaseAddVectorIndexTest`） | 见 §10 |
| `lance-spark-{3.4,3.5,4.0,4.1}_2.{12,13}/src/test/.../IncrementalAddVectorIndexTest.java` | 各版本空壳子类 | 与现有模式一致 |
| `docs/src/operations/ddl/create-index.md` | 新增 "Incremental Mode" 章节 + 表格新增 `mode` 选项行 | 文档 |

## 7. 兼容性 & 默认行为

| 场景 | 默认（现状） | mode='incremental' | mode='replace' |
|---|---|---|---|
| 索引不存在 | 创建新索引（全量） | **报错** | 创建新索引（全量） |
| 索引存在，参数一致 | 全量重建并替换 | 增量 add-only | 全量重建并替换 |
| 索引存在，参数不一致 | 全量重建并替换 | **报错** | 全量重建并替换 |
| 表无 fragment | 返回 `(0, name)` | 同 | 同 |
| 增量后 unindexed = ∅ | n/a | 返回 `(0, name)`，无事务提交 | n/a |

**向后兼容**：未指定 `mode` 完全等价于 `mode='replace'`，无任何行为变化；现有测试不需要改。

## 8. 可观测性

- `SHOW INDEXES IN <table>` 已经有 `num_unindexed_fragments` / `num_unindexed_rows`（`ShowIndexesExec.scala:87-88`），增量后这两列应回 0 或趋近 0；可作为前后断言。
- `AddIndexExec.run` 的返回行 `fragments_indexed` 在增量场景含义切换为"本次新增覆盖的 fragment 数"，需在 `docs/.../create-index.md` 明确说明。
- 增加 `logInfo(s"Incremental build: $indexName covering ${unindexedFragmentIds.size} new fragments, reusing centroids from existing $existingSegmentsCount segment(s)")`。

## 9. lance-core 依赖与风险

| 项 | 现状 | 需要新增 | 风险 |
|---|---|---|---|
| `Dataset.getIndexes()` 返回的 `Index` 含 `fragments(): Optional<List<Integer>>` | ✅ 已存在 | — | — |
| 从已有索引读出 IVF centroids `float[]` | ❌ 不可访问（仅 `indexDetails(): byte[]`） | **需新增 `Dataset.readIvfCentroids(indexName): float[]`** 或在 `Index` 上加 `getIvfCentroids()` | **阻塞 R2 上线**；如未及时上游合入，本期降级为 R1 |
| 从已有索引读出 PQ codebook | ❌ 同上 | **需新增 `Dataset.readPqCodebook(indexName): float[]`** | 同上 |
| `commitExistingIndexSegments` 仅删除重叠段 | ✅ 已是契约 | — | 需契约书面化（在我们这里加单测固化行为） |
| `dataset.createIndex(withFragmentIds(...).replace(true).withIndexName(name))` 在已有同名索引时是否抛 "already exists" | ⚠️ 现有注释（`VectorIndexJob.scala:255`）说明 `replace=true` 是为绕过此校验 | 需验证 add-only 路径下行为一致 | 中；用 R1/R2 都要先 e2e 验证 |
| `dataset.optimizeIndices(OptimizeOptions)` 行为 | ✅ 存在 | — | R1 兜底依赖 |

**强烈建议**：在动手前对 lance-core 提一个 minimal API 提案 issue，明确 R2 所需读取接口。在 lance-core 合入前，先按 R1 出 PR，把 SQL 表面、参数校验、文档、测试全部就位。

> 详细的 lance-core 跨语言 API（Rust / Java JNI / Python pyo3）设计见同目录下 `lance-core-centroids-codebook-reader-api.md`。

## 10. 测试计划

新建 `BaseIncrementalAddVectorIndexTest`（仿 `BaseAddVectorIndexTest`），核心用例：

| 用例 | 验证点 |
|---|---|
| `testIncrementalCoversNewFragmentsOnly` | 先全量建 4 frag，写入 4 frag → 增量调用 → SHOW INDEXES `num_unindexed_fragments=0`；现有段 UUID 不变；新段 UUID 不在旧集合 |
| `testIncrementalReusesIvfCentroids` | 先训练种子 A 的 centroids；增量后查询同向量 top-K，与全量重建结果对比，距离值差 ≤ 极小阈值（验证 centroids 确实复用） |
| `testIncrementalNoOpWhenAllCovered` | 全量建索引后立即增量 → 返回 `(0, name)`，未产生新事务（dataset version 不变） |
| `testIncrementalRejectsParamMismatch` | 全量建 `num_partitions=4`；增量请求 `num_partitions=8` → 抛 `IllegalArgumentException` 含 `num_partitions` |
| `testIncrementalRejectsWhenIndexAbsent` | 直接 `mode='incremental'`，未事先 CREATE → 报错 |
| `testIncrementalEmptyTable` | 无 fragment → `(0, name)` |
| `testIncrementalAtomicity` | 注入 executor 失败 → manifest 不变，旧索引可读、查询正常 |
| `testIncrementalForAllIvfVariants` | IVF_FLAT / IVF_PQ / IVF_SQ / IVF_HNSW_PQ / IVF_HNSW_SQ 各跑一遍 |
| `testIncrementalThenReplace` | 增量后再 `mode='replace'` → 段 UUID 全部刷新，行为与现状一致 |
| `testIncrementalCommitDoesNotRemoveOldSegments`（最关键） | 用 mock dataset 或观察 commit 前后 segment list 的差集 = ∅ for old，新增 = new only |

每个 Spark/Scala 矩阵（3.4_2.12 / 3.5_2.12 / 4.0_2.13 / 4.1_2.13）放空壳子类，沿用现仓 pattern。

## 11. 实施里程碑

```
M1 (≈ 1 周, R1 路线):
  - 方案 A SQL 表面 + Phase 0 差分 + 参数校验
  - 后端落到 dataset.optimizeIndices(setIndexNames=[name], retrain=false)
  - 完整测试套 + 文档
  → 用户层面已经有"增量"语义,只是 driver-only 不分布式

M2 (R2 路线, 解锁后):
  - lance-core 暴露 centroids/codebook 读取 API (见 lance-core-centroids-codebook-reader-api.md)
  - VectorIndexJob 加 skipTraining 分支
  - 同一组测试用例直接复用,结果应等价

M3 (扩展):
  - 方案 B grammar (OPTIMIZE INDEX) 作为别名
  - 段合并 (segment compaction) 子命令
  - SHOW INDEXES 增加列 segment_count
```

## 12. 风险与待解问题

1. **训练复用前提**：lance-core 是否暴露 centroids / codebook 读取接口直接决定 R2 何时落地。**需先与 lance-core 维护者对齐**，详见 `lance-core-centroids-codebook-reader-api.md`。
2. **段碎片化**：每次增量都新增一段，长期下来段数爆炸，影响读路径。需要后续提供段合并能力（M3）。短期通过 `num_segments` 控制单次增量产出段数。
3. **R1 单 driver 性能**：当 unindexed fragment 量大时，R1 不分布式可能成为瓶颈；用文档明示，超过阈值时建议先做一次 `mode='replace'` 全量。
4. **`mode` 参数命名**：`mode='incremental'` vs `mode='append'` vs 新关键字 `INCREMENTAL`；倾向 `mode='incremental'`，因为不污染 grammar。
5. **SQL 输出列语义切换**：`fragments_indexed` 在 incremental 下是"本次增量"，在 replace 下是"全表"。需要文档强调；或新增列 `previously_indexed_fragments` 让两种模式下含义都自洽（次要决策，可二期）。

