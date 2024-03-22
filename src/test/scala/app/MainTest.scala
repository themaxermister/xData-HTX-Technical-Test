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

import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import util.DataGenerators
import util.Session.{createParquetFile, createSparkSession, readParquetFile}
import util.Structures.locationRowEncoder
import util.Utils.{DETECTION_SIZE, LOCATION_SIZE, deleteRecursively}

@TestInstance(Lifecycle.PER_CLASS)
class MainTest {
  private val parquetFilePath: String = "src/test/resources/"
  private val dataGen: DataGenerators =
    DataGenerators(LOCATION_SIZE, DETECTION_SIZE)

  private val cut = 1000
  private val limit = 10
  private val locationRecordsPath: String =
    parquetFilePath + "locations.parquet/"
  private val detectionRecordsPath: String =
    parquetFilePath + "detection.parquet/"
  private val itemRecordsPath: String = parquetFilePath + "itemRank.parquet/"

  @BeforeAll
  def setup(): Unit = {
    // Remove all past files
    val file = new java.io.File(parquetFilePath)
    deleteRecursively(file)
    assert(!file.exists)

    val spark = createSparkSession()

    // Create location parquet file
    val locationRecords = dataGen.generateLocationData()
    val locationRecordsRDD = spark.sparkContext.parallelize(locationRecords)
    createParquetFile(locationRecordsRDD, locationRecordsPath)(
      spark,
      locationRowEncoder
    )
    val locationFile = new java.io.File(locationRecordsPath)
    assert(locationFile.exists)

    // Create detection parquet file
    val detectionRecords = dataGen.generateDetectionData(cut)
    val detectionRecordsRDD = spark.sparkContext.parallelize(detectionRecords)

    createParquetFile(detectionRecordsRDD, detectionRecordsPath)(
      spark,
      locationRowEncoder
    )
    val detectionFile = new java.io.File(detectionRecordsPath)
    assert(detectionFile.exists)

    spark.close()
  }

  @Test
  def testMain(): Unit = {
    Main.main(
      Array(
        locationRecordsPath,
        detectionRecordsPath,
        itemRecordsPath,
        limit.toString
      )
    )
    val itemRecordsFile = new java.io.File(itemRecordsPath)
    assert(itemRecordsFile.exists)

    val spark = createSparkSession()
    val rdd = readParquetFile(itemRecordsPath)(spark)
    assert(rdd.count() == limit)
    spark.close()
  }
}
