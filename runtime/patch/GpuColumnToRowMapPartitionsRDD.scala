/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Extracted VERBATIM from spark-rapids v26.02.0,
// sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/InternalColumnarRddConverter.scala
// (lines 760-773), into a standalone file so it can be recompiled on its own against Cloudera's
// spark-core without building the whole 773-line original — which drags in the rest of the plugin.
// The class body is unchanged. Only the imports are narrowed to what this class alone needs.
//
// See ../README-patch.md for why this is recompiled at all.

package org.apache.spark.sql.rapids.execution

import org.apache.spark.TaskContext
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuColumnToRowMapPartitionsRDD(
    prev: RDD[ColumnarBatch],
    // (TaskContext, partition index, iterator)
    f: (TaskContext, Int, Iterator[ColumnarBatch]) => Iterator[InternalRow],
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
  extends MapPartitionsRDD[InternalRow, ColumnarBatch](
    prev,
    f,
    preservesPartitioning,
    isFromBarrier,
    isOrderSensitive) {
}
