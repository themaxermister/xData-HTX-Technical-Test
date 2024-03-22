// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.graalvm.compiler.core.common.NumUtil
import util.Session.{createParquetFile, createSparkSession, readParquetFile}
import util.Structures.Partitions.partitionData
import util.Structures.{broadcastHashJoin, getTopXItemsDetected}
import util.Utils._

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Usage: scala Main <input_path_1> <input_path_2> <output_path_3>"
      )
    }

    val input_path_1 = args(0)
    validFilePath(input_path_1)

    val input_path_2 = args(1)
    validFilePath(input_path_1)

    val output_path_3 = args(2)
    validFilePath(input_path_1)

    val limit = args(3).toInt
    NumUtil.isInt(limit)

    val spark: SparkSession = createSparkSession()

    val locationRDD: RDD[Row] = readParquetFile(input_path_1)(spark)
    val partitionLocationRDD: RDD[Row] =
      partitionData(locationRDD, NUM_PARTITIONS, geographicalOidDetectionIndex)

    val detectionRDD: RDD[Row] = readParquetFile(input_path_2)(spark)

    val mergedLocationDetectionRDD: RDD[Row] = broadcastHashJoin(
      partitionLocationRDD,
      detectionRDD,
      geographicalOidDetectionIndex,
      geographicalOidLocationIndex
    )

    val itemRankRDD: RDD[Row] =
      getTopXItemsDetected(
        mergedLocationDetectionRDD,
        limit,
        geographicalLocationNameIndex,
        detectionOidIndex,
        itemNameIndex
      )

    createParquetFile(itemRankRDD, output_path_3)(
      spark,
      util.Structures.itemRankRowEncoder
    )

    spark.close()
  }

}
