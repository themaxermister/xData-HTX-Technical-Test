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
import org.apache.spark.sql.Row
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{Test, TestInstance}
import util.Session.createSparkSession
import util.Structures.{broadcastHashJoin, getTopXItemsDetected}
import util.Utils._

@TestInstance(Lifecycle.PER_CLASS)
class StructuresTest {
  private val limit = 5

  private val sampleDetectionRecords: Seq[Row] = Seq(
    Row(10.toLong, 1.toLong, 11.toLong, "Item A", 100.toLong),
    Row(10.toLong, 1.toLong, 12.toLong, "Item A", 100.toLong),
    Row(10.toLong, 1.toLong, 12.toLong, "Item A", 100.toLong),
    Row(10.toLong, 1.toLong, 13.toLong, "Item A", 100.toLong),
    Row(10.toLong, 1.toLong, 14.toLong, "Item A", 100.toLong),
    Row(10.toLong, 1.toLong, 15.toLong, "Item D", 100.toLong),
    Row(10.toLong, 1.toLong, 16.toLong, "Item D", 100.toLong),
    Row(10.toLong, 1.toLong, 17.toLong, "Item D", 100.toLong),
    Row(10.toLong, 1.toLong, 18.toLong, "Item D", 100.toLong),
    Row(10.toLong, 1.toLong, 19.toLong, "Item D", 100.toLong),
    Row(20.toLong, 1.toLong, 21.toLong, "Item B", 100.toLong),
    Row(20.toLong, 1.toLong, 21.toLong, "Item B", 100.toLong),
    Row(20.toLong, 1.toLong, 22.toLong, "Item B", 100.toLong),
    Row(20.toLong, 1.toLong, 23.toLong, "Item B", 100.toLong),
    Row(30.toLong, 1.toLong, 31.toLong, "Item C", 100.toLong),
    Row(30.toLong, 1.toLong, 32.toLong, "Item C", 100.toLong),
    Row(30.toLong, 1.toLong, 33.toLong, "Item D", 100.toLong),
    Row(30.toLong, 1.toLong, 34.toLong, "Item D", 100.toLong)
  )

  private val expectedItemRecords: Seq[Row] = Array(
    Row(10.toLong, "Item D", 1),
    Row(10.toLong, "Item A", 2),
    Row(20.toLong, "Item B", 3),
    Row(30.toLong, "Item D", 4),
    Row(30.toLong, "Item C", 5)
  )

  private val sampleLocationRecords: Seq[Row] = Seq(
    Row(10.toLong, "Location A"),
    Row(20.toLong, "Location B"),
    Row(30.toLong, "Location C"),
    Row(40.toLong, "Location D")
  )

  private val expectedMergeDetectionLocationRecords: Seq[Row] = Seq(
    Row(
      10.toLong,
      1.toLong,
      11.toLong,
      "Item A",
      100.toLong,
      10.toLong,
      "Location A"
    ),
    Row(
      10.toLong,
      1.toLong,
      12.toLong,
      "Item A",
      100.toLong,
      10.toLong,
      "Location A"
    ),
    Row(
      10.toLong,
      1.toLong,
      12.toLong,
      "Item A",
      100.toLong,
      10.toLong,
      "Location A"
    ),
    Row(
      10.toLong,
      1.toLong,
      13.toLong,
      "Item A",
      100.toLong,
      10.toLong,
      "Location A"
    ),
    Row(
      20.toLong,
      1.toLong,
      21.toLong,
      "Item B",
      100.toLong,
      20.toLong,
      "Location B"
    ),
    Row(
      20.toLong,
      1.toLong,
      21.toLong,
      "Item B",
      100.toLong,
      20.toLong,
      "Location B"
    ),
    Row(
      20.toLong,
      1.toLong,
      22.toLong,
      "Item B",
      100.toLong,
      20.toLong,
      "Location B"
    ),
    Row(
      20.toLong,
      1.toLong,
      23.toLong,
      "Item B",
      100.toLong,
      20.toLong,
      "Location B"
    ),
    Row(
      30.toLong,
      1.toLong,
      31.toLong,
      "Item C",
      100.toLong,
      30.toLong,
      "Location C"
    ),
    Row(
      30.toLong,
      1.toLong,
      32.toLong,
      "Item C",
      100.toLong,
      30.toLong,
      "Location C"
    ),
    Row(
      30.toLong,
      1.toLong,
      33.toLong,
      "Item D",
      100.toLong,
      30.toLong,
      "Location C"
    ),
    Row(
      30.toLong,
      1.toLong,
      34.toLong,
      "Item D",
      100.toLong,
      30.toLong,
      "Location C"
    )
  )

  @Test
  def testBroadcastHashJoin(): Unit = {
    val spark = createSparkSession()
    val smallerRdd = spark.sparkContext.parallelize(sampleLocationRecords)
    val largerRdd = spark.sparkContext.parallelize(sampleDetectionRecords)
    val joinedRdd = broadcastHashJoin(
      smallerRdd,
      largerRdd,
      geographicalOidDetectionIndex,
      geographicalOidLocationIndex
    )
    assert(joinedRdd.count() == sampleDetectionRecords.length)

    val expectedRDD =
      spark.sparkContext.parallelize(expectedMergeDetectionLocationRecords)
    assert(
      joinedRdd.collect().sameElements(expectedRDD.collect()),
      "Broadcast hash join result doesn't match the expected result"
    )
    spark.close()
  }

  @Test
  def testGetTopXItems(): Unit = {
    val spark = createSparkSession()
    val rdd = spark.sparkContext.parallelize(sampleDetectionRecords)

    val topXItems: RDD[Row] =
      getTopXItemsDetected(
        rdd,
        limit,
        geographicalOidDetectionIndex,
        detectionOidIndex,
        itemNameIndex
      )

    val expectedRDD = spark.sparkContext.parallelize(expectedItemRecords)
    assert(
      topXItems.collect().sameElements(expectedRDD.collect()),
      "Top X items result doesn't match the expected result"
    )
    spark.close()
  }
}
