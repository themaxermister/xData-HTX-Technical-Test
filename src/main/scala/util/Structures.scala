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

package util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, Row}

object Structures {
  val detectionRowEncoder: Encoder[Row] = Encoders.row(
    StructType(
      List(
        StructField("geographical_location_oid", LongType, nullable = false),
        StructField("video_camera_oid", LongType, nullable = false),
        StructField("detection_oid", LongType, nullable = false),
        StructField("item_name", StringType, nullable = false),
        StructField("timestamp_detected", LongType, nullable = false)
      )
    )
  )
  val locationRowEncoder: Encoder[Row] = Encoders.row(
    StructType(
      List(
        StructField("geographical_location_oid", LongType, nullable = false),
        StructField("geographical_location", StringType, nullable = false)
      )
    )
  )
  val itemRankRowEncoder: Encoder[Row] = Encoders.row(
    StructType(
      List(
        StructField("geographical_location", LongType, nullable = false),
        StructField("item_rank", StringType, nullable = false),
        StructField("item_name", StringType, nullable = false)
      )
    )
  )

  def getTopXItemsDetected(
      rdd: RDD[Row],
      limit: Int,
      locationColIdx: Int,
      detectionIdColIdx: Int,
      itemColIdx: Int
  ): RDD[Row] = {

    val distinctRDD = rdd
      .map(row =>
        ((row(locationColIdx), row(detectionIdColIdx)), row(itemColIdx))
      )
      .reduceByKey((x, y) => x)

    val locationItemCounts =
      distinctRDD
        .map(row => ((row._1._1, row._2), 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(limit)

    val locationItemRank = locationItemCounts.zipWithIndex
      .map { case ((location_item, count), rank) =>
        (location_item, count, rank + 1)
      }

    val itemRankRecords = locationItemRank.map {
      case (location_item, item, rank) =>
        Row(location_item._1, location_item._2, rank)
    }

    rdd.context.parallelize(itemRankRecords)
  }

  def broadcastHashJoin(
      smallerRdd: RDD[Row],
      largerRdd: RDD[Row],
      smallerRddColumnIdx: Int,
      largerRddColumnIdx: Int
  ): RDD[Row] = {
    val broadcastSmallerRedMap = smallerRdd
      .map(row => row.getLong(smallerRddColumnIdx) -> row)
      .collectAsMap()
    val broadcastVar = largerRdd.context.broadcast(broadcastSmallerRedMap)
    val joinedRdd = largerRdd.mapPartitions { iter =>
      val map = broadcastVar.value
      iter.flatMap { row =>
        map
          .get(row.getLong(largerRddColumnIdx))
          .map(x => Row.fromSeq(row.toSeq ++ x.toSeq))
      }
    }
    joinedRdd
  }

  object Partitions {
    def partitionData(
        rdd: RDD[Row],
        numPartitions: Int,
        colIndex: Int
    ): RDD[Row] = {
      val partitioner = new Partitions.LocationPartitioner(numPartitions)
      val partitionedData = rdd
        .map { x: Row => (x(colIndex), x) }
        .map(x => (x._1, x._2))
        .partitionBy(partitioner)
        .persist()

      val totalRows =
        partitionedData.mapPartitions(iter => Iterator(iter.size)).sum()
      if (totalRows != rdd.count()) {
        throw new IllegalStateException(
          "Partitioned data does not match original data"
        )
      }
      partitionedData.map { case (_, row) => row }
    }

    private class LocationPartitioner(partitions: Int)
        extends org.apache.spark.Partitioner
        with Serializable {
      override def numPartitions: Int = partitions

      override def getPartition(key: Any): Int = key match {
        case loc: Long => (loc.hashCode & Integer.MAX_VALUE) % partitions
        case _         => throw new IllegalArgumentException("Key is not a Long")
      }
    }
  }

}
