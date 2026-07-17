# Lance Core: Centroids / Codebook Reader API across Rust / Java / Python

- **Status**: Draft (cross-repo)
- **Owner**: lance-core
- **Caller**: lance-spark-sql-vec ([incremental-vector-index.md](./incremental-vector-index.md))
- **Last updated**: 2026-06-17

## 0. 目标

为已提交的向量索引（IVF_FLAT / IVF_PQ / IVF_SQ / IVF_HNSW_PQ / IVF_HNSW_SQ）暴露两个**只读**接口，按索引名读出：

1. **IVF centroids** —— 训练得到的中心点
2. **PQ codebook** —— 仅 IVF_PQ / IVF_HNSW_PQ 有

接口必须在 Rust core、Java JNI 和 Python pyo3 三层全部暴露，签名风格与各语言现有惯例一致。**这是 lance-spark 增量分布式向量索引构建（Phase B / R2 路线）的硬依赖**。

## 1. 概览

```
                  ┌────────────────────────────────────────────┐
                  │  lance-spark-sql-vec  (Java consumer)       │
                  │  IncrementalVectorIndexJob (driver) →       │
                  │    dataset.readIvfCentroids("vec_idx")      │
                  │    dataset.readPqCodebook("vec_idx")        │
                  └────────────────┬───────────────────────────┘
                                   │ JNI
                  ┌────────────────▼───────────────────────────┐
                  │  org.lance.Dataset  (Java public)           │
                  │  Optional<IvfCentroids> readIvfCentroids()  │
                  │  Optional<PqCodebook>  readPqCodebook()     │
                  └────────────────┬───────────────────────────┘
                                   │ Java_org_lance_Dataset_*
                  ┌────────────────▼───────────────────────────┐
   Python ───────►│  rust/lance/src/index.rs (Rust core SoT)    │
   pyarrow        │  trait DatasetIndexExt {                    │
                  │      async fn read_ivf_centroids(name)       │
                  │          -> Result<FixedSizeListArray>;      │
                  │      async fn read_pq_codebook(name)          │
                  │          -> Result<FixedSizeListArray>;       │
                  │  }                                            │
                  └─────────────────────────────────────────────┘
```

**单一事实源在 Rust core**。Java、Python 都是薄绑定。Java 因 Spark Arrow 版本兼容问题用 `float[]`（详见 §4），Python 沿用 `pa.FixedSizeListArray`（详见 §5）。

## 2. 核心数据存储位置（已落地的事实）

| 数据 | 内存表示 | 来源 trait 方法 |
|---|---|---|
| IVF centroids | `IvfModel.centroids: Option<FixedSizeListArray>`（`rust/lance-index/src/vector/ivf/storage.rs:31`） | `VectorIndex::ivf_model() -> &IvfModel`（`rust/lance-index/src/vector.rs:393`） |
| PQ codebook | `ProductQuantizer.codebook: FixedSizeListArray`（`rust/lance-index/src/vector/pq.rs:43-47`） | `VectorIndex::quantizer() -> Quantizer`（`rust/lance-index/src/vector.rs:394`），匹配 `Quantizer::Product(pq)`（`rust/lance-index/src/vector/quantizer.rs:120-126`） |
| 索引段聚合 | `LogicalVectorIndex { name, column, segments: Vec<(IndexMetadata, Arc<dyn VectorIndex>)> }`（`rust/lance/src/index/vector.rs:71-76`） | `Dataset::open_logical_vector_index(column, name)`（`rust/lance/src/index.rs:1621-1625`） |

**关键观察**：现有路径已经把磁盘格式版本差异（`(0,0)`/`(0,1)` 旧版、`(0,2)` 中间版、`(0,3)`/`(2,_)` V3）封装在 `open_vector_index` / `open_logical_vector_index` 内部。新接口**走这条已有路径**，自动免费支持所有版本。

格式版本来源：`rust/lance/src/index.rs:1775-1776`（`read_version`）+ `1791-2008`（dispatch）。`retain_supported_indices` 在 `rust/lance/src/index.rs:1569-1592`。

## 3. Rust core API 设计

### 3.1 公开位置 —— `DatasetIndexExt`（公共 trait）

> 文件：`rust/lance/src/index/api.rs`

```rust
#[async_trait]
pub trait DatasetIndexExt {
    // ... existing methods ...

    /// Read the trained IVF centroids of a committed vector index by name.
    ///
    /// Returns the union of every segment's centroids, in segment commit order.
    /// Each list element is one centroid; the inner list size equals the indexed
    /// vector dimension. Element type matches the indexed column
    /// (`Float16` / `Float32` / `Float64` / `UInt8`).
    ///
    /// # Errors
    /// - [`Error::IndexNotFound`] when no logical index with this name exists.
    /// - [`Error::Index`] when the index is not vector-typed, has no IVF model,
    ///   or segments disagree on dimension.
    async fn read_ivf_centroids(&self, index_name: &str) -> Result<FixedSizeListArray>;

    /// Read the trained PQ codebook of a committed IVF_PQ / IVF_HNSW_PQ index.
    ///
    /// Length = `num_sub_vectors * 2.pow(num_bits)`; inner list length =
    /// `dimension / num_sub_vectors`; element type = `Float32` (PQ codebooks
    /// are always loaded as f32 — see `ProductQuantizer::from_metadata`).
    ///
    /// # Errors
    /// - [`Error::IndexNotFound`] when the index name is unknown.
    /// - [`Error::NotSupported`] when the index does not use Product Quantization
    ///   (`IVF_FLAT` / `IVF_SQ` / `IVF_HNSW_FLAT` / `IVF_HNSW_SQ` / `IVF_RQ`).
    /// - [`Error::Index`] when segments disagree on PQ shape.
    async fn read_pq_codebook(&self, index_name: &str) -> Result<FixedSizeListArray>;
}
```

### 3.2 实现 —— `rust/lance/src/index/vector.rs` 上的 `LogicalIvfView` 方法

```rust
impl<'a> LogicalIvfView<'a> {
    pub fn read_centroids(&self) -> Result<FixedSizeListArray> {
        let mut parts = Vec::with_capacity(self.logical_index.num_segments());
        let mut dim: Option<i32> = None;
        for index in self.indices() {
            let arr = index.ivf_model().centroids_array()
                .ok_or_else(|| Error::index(format!(
                    "Logical index '{}': segment is missing IVF centroids",
                    self.logical_index.name())))?;
            if let Some(d) = dim {
                if d != arr.value_length() {
                    return Err(Error::index(format!(
                        "Logical index '{}': segments disagree on dimension ({} vs {})",
                        self.logical_index.name(), d, arr.value_length())));
                }
            } else {
                dim = Some(arr.value_length());
            }
            parts.push(arr.clone());
        }
        let refs: Vec<&dyn Array> = parts.iter().map(|a| a as &dyn Array).collect();
        let combined = arrow::compute::concat(&refs)?;
        Ok(combined.as_fixed_size_list().clone())
    }

    pub fn read_pq_codebook(&self) -> Result<FixedSizeListArray> {
        let mut codebook: Option<FixedSizeListArray> = None;
        for index in self.indices() {
            let pq = match index.quantizer() {
                Quantizer::Product(pq) => pq,
                other => return Err(Error::NotSupported {
                    source: format!(
                        "Index '{}' uses {} quantization, not PQ",
                        self.logical_index.name(), other.quantization_type()).into(),
                    location: location!(),
                }),
            };
            match codebook.as_ref() {
                None => codebook = Some(pq.codebook.clone()),
                Some(prev) => {
                    if prev.value_length() != pq.codebook.value_length()
                        || prev.len() != pq.codebook.len() {
                        return Err(Error::index(format!(
                            "Logical index '{}': segments disagree on PQ codebook shape",
                            self.logical_index.name())));
                    }
                }
            }
        }
        codebook.ok_or_else(|| Error::index(format!(
            "Logical index '{}': no segments", self.logical_index.name())))
    }
}
```

### 3.3 `DatasetIndexExt` 默认实现（位置：`rust/lance/src/index.rs`）

```rust
async fn read_ivf_centroids(&self, index_name: &str) -> Result<FixedSizeListArray> {
    let metadatas = self.load_indices_by_name(index_name).await?;
    if metadatas.is_empty() {
        return Err(Error::index_not_found(format!("name={index_name}")));
    }
    let column = self.schema().field_by_id(metadatas[0].fields[0])
        .ok_or_else(|| Error::index(format!(
            "Index '{index_name}': index column not in current schema")))?;
    let logical = self.open_logical_vector_index(&column.name, index_name).await?;
    logical.as_ivf()?.read_centroids()
}

async fn read_pq_codebook(&self, index_name: &str) -> Result<FixedSizeListArray> {
    let metadatas = self.load_indices_by_name(index_name).await?;
    if metadatas.is_empty() {
        return Err(Error::index_not_found(format!("name={index_name}")));
    }
    let column = self.schema().field_by_id(metadatas[0].fields[0])
        .ok_or_else(|| Error::index(format!(
            "Index '{index_name}': index column not in current schema")))?;
    let logical = self.open_logical_vector_index(&column.name, index_name).await?;
    logical.as_ivf()?.read_pq_codebook()
}
```

### 3.4 返回类型论证

| 候选 | 选择 | 原因 |
|---|---|---|
| `FixedSizeListArray` | ✅ | 内存中本就是这个类型；`value_length()` 直接给维度，`len()` 给 partition 数；保留元素类型（f16/f32/f64/u8） |
| `Float32Array` | ❌ | 丢失维度结构；与项目其余地方不一致 |
| 自定义结构 `IvfCentroidsArtifact` | 留给 v2 | DistanceType 在 `IndexMetadata.index_details` 已能拿到；先窄做 |

### 3.5 错误语义对齐

- 索引不存在 → `Error::index_not_found(format!("name={index_name}"))`，匹配 `rust/lance/src/index.rs:1325`/`2035` 现有用法
- PQ 类型不匹配 → `Error::NotSupported`，让 Python `infer_error()` 自动映射为 `PyNotImplementedError`
- 段不一致 → `Error::index(...)` 含具体差异
- 版本过新 → 由 `retain_supported_indices` 在 `load_indices()` 阶段过滤，新接口无需重复检查

## 4. Java JNI API 设计

### 4.1 公开位置 —— `org.lance.Dataset`

> 文件：`java/src/main/java/org/lance/Dataset.java`（包名是 `org.lance`，**不是** `com.lancedb.lance`，参见前序 JNI 调研）

```java
/**
 * Read the trained IVF centroids of a vector index built on this dataset.
 *
 * @param indexName logical index name
 * @return centroids POJO; empty when the index exists but has no IVF model
 * @throws IllegalArgumentException when no index named {@code indexName} exists
 */
public Optional<IvfCentroids> readIvfCentroids(String indexName) {
  try (LockManager.LockGuard guard = lockManager.acquireReadLock()) {
    Preconditions.checkArgument(nativeDatasetHandle != 0, "Dataset is closed");
    return Optional.ofNullable(nativeReadIvfCentroids(indexName));
  }
}

private native IvfCentroids nativeReadIvfCentroids(String indexName);

/**
 * Read the trained PQ codebook of an IVF_PQ / IVF_HNSW_PQ index.
 *
 * @return codebook POJO; empty when the index exists but is not PQ-quantized
 * @throws IllegalArgumentException when no index named {@code indexName} exists
 */
public Optional<PqCodebook> readPqCodebook(String indexName) {
  try (LockManager.LockGuard guard = lockManager.acquireReadLock()) {
    Preconditions.checkArgument(nativeDatasetHandle != 0, "Dataset is closed");
    return Optional.ofNullable(nativeReadPqCodebook(indexName));
  }
}

private native PqCodebook nativeReadPqCodebook(String indexName);
```

### 4.2 配套 POJO（位置：`java/src/main/java/org/lance/index/vector/`）

```java
public final class IvfCentroids {
  private final float[] flat;          // [numPartitions * dimension] row-major
  private final int numPartitions;
  private final int dimension;
  private final String elementType;    // "FLOAT32" | "FLOAT16" | "FLOAT64" | "UINT8"

  // ctor + getters; no setters; final fields
}

public final class PqCodebook {
  private final float[] flat;          // row-major flatten;
                                       //   row layout: subvec 0 cents, then subvec 1, ...
  private final int numSubVectors;
  private final int numBits;            // typically 8
  private final int subVectorDim;       // == dimension / numSubVectors
}
```

### 4.3 返回类型论证（关键决策）

**为什么不返回 Arrow Java `FixedSizeListVector`**：
- `lance-core` 编译于 Arrow Java 18.3.0（`/Users/zhoubin11/projects/lance-sql-vec/java/pom.xml:31`）
- Spark 3.5 类路径上是 Arrow 15.0.2（`lance-spark-sql-vec/pom.xml:59`），Spark 4.0 是 Arrow 18.x
- 暴露 `FixedSizeListVector` 强制 Spark 3.x 用户也升级 Arrow 18，跨 BufferAllocator 边界有 `LinkageError`/`NoSuchMethodError` 风险
- 现有 `VectorTrainer.trainIvfCentroids` / `trainPqCodebook` 都已经返回 `float[]`（参见 `vector_trainer.rs:84-145, 147-203`，`flatten_fixed_size_list_to_f32` at `vector_trainer.rs:22-35`）—— **保持对称**

**为什么不直接 `float[]` 而要包 POJO**：
- `float[]` 丢失行结构（dim、numPartitions），调用方必须从外部拿到
- 把维度信息和数组绑在一起，让 SQL/Spark 调用方一次性拿到自洽 view

**为什么 `Optional<>`**：
- 索引存在但不是 IVF / 不是 PQ → `Optional.empty()`
- 索引名不存在 → 抛 `IllegalArgumentException`（与 `getIndexStatistics` 一致）
- 现有 `nativeMemWalIndexDetails`（`mem_wal.rs:1008-1028`，对应 Java 方法 `Dataset.java:2055-2060`）就是这个 pattern

### 4.4 JNI 桥实现

> 文件：`java/lance-jni/src/blocking_dataset.rs`

```rust
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_Dataset_nativeReadIvfCentroids<'local>(
    mut env: JNIEnv<'local>,
    java_dataset: JObject<'local>,
    jindex_name: JString<'local>,
) -> JObject<'local> {
    ok_or_throw!(env, inner_read_ivf_centroids(&mut env, java_dataset, jindex_name))
}

fn inner_read_ivf_centroids<'local>(
    env: &mut JNIEnv<'local>,
    java_dataset: JObject<'local>,
    jindex_name: JString<'local>,
) -> Result<JObject<'local>> {
    let index_name = JString::extract(env, jindex_name)?;
    let dataset_guard = unsafe {
        env.get_rust_field::<_, _, BlockingDataset>(&java_dataset, NATIVE_DATASET)?
    };
    let dataset = dataset_guard.inner.clone();

    let centroids = match RT.block_on(dataset.read_ivf_centroids(&index_name)) {
        Ok(arr) => arr,
        Err(LanceError::IndexNotFound { .. }) => {
            return Err(LanceError::input_error(format!(
                "Index '{index_name}' does not exist")).into());
        }
        Err(LanceError::NotSupported { .. }) | Err(LanceError::Index { .. }) => {
            // Wrong index type / no centroids => return null => Optional.empty
            return Ok(JObject::null());
        }
        Err(e) => return Err(e.into()),
    };

    // Reuse flatten_fixed_size_list_to_f32 (vector_trainer.rs:22-35)
    let dim = centroids.value_length();
    let num_partitions = centroids.len();
    let flat = flatten_fixed_size_list_to_f32(&centroids)?;
    let elem_type = arrow_type_to_jstring(env, centroids.value_type())?;

    let cls = env.find_class("org/lance/index/vector/IvfCentroids")?;
    let jflat = env.new_float_array(flat.len() as i32)?;
    env.set_float_array_region(&jflat, 0, &flat)?;
    let obj = env.new_object(
        cls,
        "([FIILjava/lang/String;)V",
        &[(&jflat).into(), JValue::Int(num_partitions as i32),
          JValue::Int(dim), (&elem_type).into()],
    )?;
    Ok(obj)
}
```

PQ codebook 桥逻辑同形：
- 调 `dataset.read_pq_codebook(&index_name)`
- `NotSupported` / 量化器不匹配 → 返回 `JObject::null()` → Java `Optional.empty`
- `num_bits` 通过 `IndexMetadata.index_details`（已可解析为 `pb::VectorIndexDetails`）或 `getIndexStatistics` 取得；目前最简：在 Rust 侧根据 `pq.num_bits` 字段直接返回

### 4.5 错误传播

复用现有 `ok_or_throw!`（`java/lance-jni/src/lib.rs:5-15`）+ `Error::throw`（`error.rs:88-116`）+ `From<LanceError>`（`error.rs:176-202`）链路。映射：

| Rust 错误 | Java 行为 |
|---|---|
| `IndexNotFound` | `IllegalArgumentException` |
| `NotSupported` / `Index { .. }` | 返回 null `JObject` → Java `Optional.empty` |
| 其他 | `RuntimeException`（按现有 `From<LanceError>`） |

> "类型不匹配走 `Optional.empty` 而非异常"是为了让调用方写 `optional.ifPresent(...)` 而非 try-catch。

## 5. Python pyo3 API 设计

### 5.1 公开位置 —— `lance.LanceDataset`

> 文件：`python/python/lance/dataset.py`（在 `prewarm_index` 附近，~line 3969）

```python
def read_ivf_centroids(self, index_name: str) -> pa.FixedSizeListArray:
    """Read the trained IVF centroids of a vector index.

    Returns
    -------
    pa.FixedSizeListArray
        Length = num_partitions, inner list size = vector dimension.
        Element type matches the indexed column (typically ``float32``).

    Raises
    ------
    ValueError
        When no index with this name exists, or when the index is not vector-typed.
    """
    return self._ds.read_ivf_centroids(index_name)


def read_pq_codebook(self, index_name: str) -> pa.FixedSizeListArray:
    """Read the trained PQ codebook of an IVF_PQ / IVF_HNSW_PQ index.

    Returns
    -------
    pa.FixedSizeListArray
        ``num_sub_vectors * 2 ** num_bits`` rows of length
        ``dimension / num_sub_vectors``; element type ``float32``.

    Raises
    ------
    NotImplementedError
        When the index does not use Product Quantization
        (e.g. IVF_FLAT, IVF_SQ, IVF_HNSW_SQ).
    ValueError
        When no index with this name exists.
    """
    return self._ds.read_pq_codebook(index_name)
```

### 5.2 pyo3 桥实现

> 文件：`python/src/dataset.rs`（在 `#[pymethods] impl Dataset` 块内，靠近 `prewarm_index` ~line 2532）

```rust
/// Read the trained IVF centroids for a vector index.
fn read_ivf_centroids<'py>(
    &self, py: Python<'py>, index_name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let centroids = rt().block_on(Some(py), self.ds.read_ivf_centroids(index_name))?
        .infer_error()?;
    centroids.into_data().to_pyarrow(py)
}

/// Read the trained PQ codebook for a vector index.
fn read_pq_codebook<'py>(
    &self, py: Python<'py>, index_name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let codebook = rt().block_on(Some(py), self.ds.read_pq_codebook(index_name))?
        .infer_error()?;
    codebook.into_data().to_pyarrow(py)
}
```

### 5.3 返回类型论证

| 候选 | 选择 | 原因 |
|---|---|---|
| `pa.FixedSizeListArray` | ✅ | 与现有 `train_ivf_model`（`python/src/indices.rs:283`）/ `train_pq_model`（line 358）/ `PyIvfModel.centroids` 一致；`IvfModel(centroids=…)`、`PqModel(codebook=…)`（`python/lance/indices/ivf.py:16`、`pq.py:17`）已经类型为 `pa.FixedSizeListArray` |
| `np.ndarray` | ❌ | 项目无 numpy crate 依赖；丢失元素类型；fp16 强制转换。需要时调用方 `.to_numpy()` |
| `pa.RecordBatch` | ❌ | 单列裹一层 noise；天然就是 2D fixed-size-list |

### 5.4 错误传播

`PythonErrorExt::infer_error()`（`python/src/error.rs:58-115`）已配置：

| `lance::Error` | Python 异常 |
|---|---|
| `IndexNotFound` | `PyValueError`（`error.rs:79`） |
| `NotSupported` | `PyNotImplementedError`（`error.rs:77`） |
| 其他 | `PyRuntimeError` |

**无需新增 error 映射**。

### 5.5 与现有 `lance.lance.indices.get_ivf_model` 的关系

`indices.rs:764` 已注册 `get_ivf_model`，返回 `PyIvfModel`，其 `.centroids` getter 已经能产出 `Optional[pa.FixedSizeListArray]`。

但**没有对称的 `get_pq_model`**，所以 `read_pq_codebook` 必须走新 pyo3 函数。为对称起见，两者都加为 `Dataset` 上的方法，调用方 API 干净。

## 6. 兼容性矩阵

| 场景 | Rust core | Java | Python |
|---|---|---|---|
| 索引不存在 | `Error::IndexNotFound` | `IllegalArgumentException` | `ValueError` |
| 索引存在但不是向量索引 | `Error::Index` | `Optional.empty` | `ValueError` |
| `read_pq_codebook` 用在 IVF_FLAT/IVF_SQ/IVF_HNSW_SQ | `Error::NotSupported` | `Optional.empty` | `NotImplementedError` |
| 索引段维度不一致（异常） | `Error::Index` | 异常 | `ValueError` |
| 版本过新的段 | 自动被 `retain_supported_indices` 过滤 | 同 | 同 |
| 多段 IVF_PQ codebook 内容相同 | 取第一段 | 同 | 同 |
| 多段 IVF_PQ codebook 内容不同（异常） | `Error::Index` 保险 | 异常 | `ValueError` |

## 7. 测试计划

### Rust core（`rust/lance/src/index.rs` 测试块）

| 用例 | 验证 |
|---|---|
| `test_read_ivf_centroids_ivf_flat` | 创建 IVF_FLAT 索引 → `read_ivf_centroids` 返回的 dim 与训练时一致 |
| `test_read_ivf_centroids_unknown_name` | 不存在的索引名 → `Error::IndexNotFound` |
| `test_read_pq_codebook_ivf_pq` | 创建 IVF_PQ → `read_pq_codebook` 长度 = `num_sub_vectors * 2^num_bits` |
| `test_read_pq_codebook_on_ivf_flat_errors` | 在 IVF_FLAT 上调用 → `Error::NotSupported`（含 "not PQ"） |
| `test_read_centroids_multi_segment_consistent` | 分布式构建产出 N 段 → 拼接后总 partition 数 = sum |
| `test_read_pq_codebook_multi_segment_identical` | 多段 PQ codebook 相同 → 返回单一 codebook |
| `test_read_pq_codebook_multi_segment_diff_shape_errors` | 注入伪段使 codebook shape 不一致 → `Error::Index`（保险） |
| `test_legacy_ivf_pq_v1_format` | 用 v1 格式构建一次（绕过 V3）→ 仍能读出，验证版本兼容 |

### Java（`BaseReadIndexArtifactsTest.java` 新增基类）

| 用例 | 验证 |
|---|---|
| `testReadIvfCentroidsReturnsCorrectShape` | dim 与构建时一致；`numPartitions` 与 stats 一致 |
| `testReadPqCodebookReturnsCorrectShape` | `subVectorDim * numSubVectors == dim`；`flat.length == numSubVectors * 256 * subVectorDim`（8-bit） |
| `testReadIvfCentroidsUnknownIndexThrows` | `IllegalArgumentException` |
| `testReadPqCodebookOnIvfFlatReturnsEmpty` | `Optional.empty` |
| `testReadIvfCentroidsRoundTripWithVectorTrainer` | 用 `VectorTrainer.trainIvfCentroids` 训出 → 构建索引 → 读回 → 数值近似（容忍浮点误差） |

### Python（`python/python/tests/test_index_readers.py`）

| 用例 | 验证 |
|---|---|
| `test_read_ivf_centroids_returns_pyarrow_fsl` | `isinstance(result, pa.FixedSizeListArray)`；`type.list_size == dim` |
| `test_read_ivf_centroids_unknown_raises_value_error` | `pytest.raises(ValueError)` |
| `test_read_pq_codebook_on_ivf_sq_raises_not_implemented` | `pytest.raises(NotImplementedError)` |
| `test_read_pq_codebook_returns_float32` | `result.values.type == pa.float32()` |
| `test_round_trip_into_pq_model` | `PqModel(num_subvectors=..., codebook=ds.read_pq_codebook("idx"))` 构造成功 |

## 8. 性能 / 缓存说明

- 两个方法都通过 `open_logical_vector_index` → `open_vector_index`，命中索引缓存
- 首次调用：每段一次 `read_global_buffer`（centroids 几 KB ~ 几 MB；codebook `2^num_bits * dim * 4` 字节，典型 `256 * 768 * 4 = 768 KB`）
- 后续调用：内存命中
- `FixedSizeListArray::clone()` 是 Arc 引用计数 +1，**不复制数据**
- Java 端 `flatten_fixed_size_list_to_f32` + `set_float_array_region` 各拷贝一次到 JVM 堆；峰值内存 = 2x 数组大小
- Python 端 `into_data().to_pyarrow(py)` 是 Arrow C Data Interface，零拷贝

## 9. 与 lance-spark 增量构建的衔接

> 详见 [incremental-vector-index.md](./incremental-vector-index.md) §4.2 R2 路线。

```
AddIndexExec.run() (Spark driver)
  ├─ Phase 0: 检测 mode='incremental'
  ├─ 调用 dataset.readIvfCentroids(indexName).orElseThrow(...)
  ├─ 调用 dataset.readPqCodebook(indexName).orElse(empty for non-PQ)
  ├─ sc.broadcast(centroids.flat()) / sc.broadcast(codebook.flat())
  └─ VectorIndexJob.runSegments(skipTraining=true,
                                presetCentroids=..., presetCodebook=...)
```

Java 端拿到 `IvfCentroids` 后：

```java
IvfCentroids c = dataset.readIvfCentroids(indexName).orElseThrow(...);
PqCodebook   k = dataset.readPqCodebook(indexName).orElse(null);
Broadcast<float[]> cBC = sc.broadcast(c.getFlat());
Broadcast<float[]> kBC = (k != null)
    ? sc.broadcast(k.getFlat())
    : sc.broadcast(new float[0]);
```

Executor 端 `VectorIndexTask` 复用 `IvfBuildParams.Builder().setCentroids(cBC.value())` —— **零改动**，因为 `setCentroids(float[])` 已存在。

## 10. 实施顺序与阻塞依赖

```
Step 1  (Rust core PR):  rust/lance/src/index/api.rs + index.rs
                         + index/vector.rs 上 LogicalIvfView 方法
                         + 单测
                         ▼
Step 2  (Java PR):       java/lance-jni 桥 + Dataset.java 方法
                         + 两个 POJO + 基类测试        ✱ 仅依赖 Step 1
                         ▼
Step 3  (Python PR):     python/src/dataset.rs
                         + python/python/lance/dataset.py
                         + pytest                      ✱ 仅依赖 Step 1
                         ▼
Step 4  (lance-spark PR):  使用 readIvfCentroids / readPqCodebook
                           → 真正的分布式增量构建（R2 路线）
                                                       ✱ 依赖 Step 2
```

Step 2、Step 3 可以并行。Step 4 在 lance-spark 仓库进行，与本 PR 无关。

## 11. Open Questions

1. **多段 IVF_PQ codebook 是否完全字节相等？** 当前实现假设 `prepare_vector_segment_build`（`rust/lance/src/index/vector.rs:530-535`）的 `require_precomputed_ivf=true` 保证内容一致，但 commit 时未做内容相等校验。建议：先按"shape 一致就接受"放行，加测试确认实际等同。
2. **`read_ivf_centroids` 拼接 vs per-segment？** 当前选择拼接，因为 lance-spark 增量场景需要"全量 centroid 表"。如果有调用方真要 per-segment 拆分（调试用），后续加 `read_ivf_centroids_per_segment` 不破坏 v1 兼容。
3. **是否要把 `DistanceType` 一起返回？** 当前不返回，调用方走 `getIndexStatistics(name).get("metric_type")`。如果发现这是常见需求，加 `IvfCentroids.distanceType` 字段。
4. **Java POJO 是否在 `org.lance.index.vector` 包下？** 现有 `org.lance.index.scalar.ZoneStats` 是 scalar 子包；vector 子包尚未存在，需要创建。
5. **HNSW 图本身是否要暴露？** 不在本期。HNSW 图不是 centroids，结构复杂，调用方目前没有这个需求。如未来要加，单独做 `read_hnsw_graph(name, partition_id)`。
6. **`IVF_RQ` 路径**：`Quantizer::Rabit` 不返回 codebook（artifact 是旋转矩阵 `rust/lance-index/src/vector/bq.rs:106`）。本期 `read_pq_codebook` 在 RQ 上返回 `Error::NotSupported`，与 IVF_FLAT 等同处理，与实现一致。

## 12. 附录：调研报告

为节省主文档篇幅，调研细节存于：
- `_investigation-python-pyo3.md`（pyo3 调研，已落盘）
- `_investigation-java-jni.md`（Java JNI 调研，前序会话产出，结论已嵌入 §4，原始报告未落盘）
- `_investigation-rust-core.md`（Rust core 调研，agent 终止前未落盘，原始结论已嵌入本设计 §2-§3）

