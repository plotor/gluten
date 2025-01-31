/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.v2.clickhouse.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.execution.datasources.{PartitioningAwareFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.utils.MergeTreePartsPartitionsUtil
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.{FileStatus, Path}

import java.util.OptionalLong

import scala.collection.mutable

abstract class ClickHouseScanBase(
    sparkSession: SparkSession,
    @transient table: ClickHouseTableV2,
    dataSchema: StructType,
    readDataSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends FileScan {

  override def isSplitable(path: Path): Boolean = false

  /** TODO: MergeTree DS V2 can not support partitions now. */
  override def readPartitionSchema: StructType = new StructType()

  override def fileIndex: PartitioningAwareFileIndex =
    new PartitioningAwareFileIndex(sparkSession, Map.empty, None) {
      override def partitionSpec(): PartitionSpec = PartitionSpec.emptySpec

      override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] =
        mutable.LinkedHashMap.empty[Path, FileStatus]

      override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] =
        Map.empty

      override def rootPaths: Seq[Path] = Seq.empty

      override def refresh(): Unit = {}

      override def inputFiles: Array[String] = table.listFiles().map(_.path).toArray
    }

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = partsPartitions.toArray

  protected def partsPartitions: Seq[InputPartition] =
    MergeTreePartsPartitionsUtil.getPartsPartitions(sparkSession, table)

  override def createReaderFactory(): PartitionReaderFactory = {
    new ClickHousePartitionReaderFactory()
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
        val size = (compressionFactor * table.listFiles().map(_.bytesOnDisk).sum).toLong
        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def getMetaData(): Map[String, String] = {
    Map.empty[String, String]
  }

  protected def getSnapshot(): Snapshot = table.updateSnapshot()
}
