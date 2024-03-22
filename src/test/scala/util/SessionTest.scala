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

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api._
import util.Session.{createParquetFile, createSparkSession, readParquetFile}
import util.Structures.{detectionRowEncoder, itemRankRowEncoder, locationRowEncoder}
import util.Utils.{DETECTION_SIZE, LOCATION_SIZE, deleteRecursively}

@TestMethodOrder(classOf[OrderAnnotation])
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SessionTest {
  private val parquetFilePath: String = "src/test/resources/"
  private val size = 10
  private val dataGen = DataGenerators(LOCATION_SIZE, DETECTION_SIZE)

  @Test
  @Order(1)
  def testCreateSparkSession(): Unit = {
    val spark = createSparkSession()
    assert(spark != null)
    assert(spark.isInstanceOf[SparkSession])
    spark.close()
  }

  @Test
  @Order(2)
  def testCreateLocationsParquetFile(): Unit = {
    val fileLocation = parquetFilePath + "locations.parquet/"
    val spark = createSparkSession()
    val locationRecords = dataGen.generateLocationData()
    val locationRecordsRDD = spark.sparkContext.parallelize(locationRecords)

    createParquetFile(locationRecordsRDD, fileLocation)(
      spark,
      locationRowEncoder
    )
    val file = new java.io.File(fileLocation)
    assert(file.exists)
    spark.close()
  }

  @Test
  @Order(3)
  def testReadLocationsParquetFile(): Unit = {
    val fileLocation = parquetFilePath + "locations.parquet/"
    val spark = createSparkSession()
    val rdd = readParquetFile(fileLocation)(spark)
    assert(rdd.count() == size)
    spark.close()
  }

  @Test
  @Order(4)
  def testCreateDetectionParquetFile(): Unit = {
    val fileLocation = parquetFilePath + "detections.parquet/"
    val spark = createSparkSession()
    val detectionRecords = dataGen.generateDetectionData(size)
    val detectionRecordsRDD = spark.sparkContext.parallelize(detectionRecords)

    createParquetFile(detectionRecordsRDD, parquetFilePath + fileLocation)(
      spark,
      detectionRowEncoder
    )

    val file = new java.io.File(parquetFilePath)
    assert(file.exists)
    spark.close()
  }

  @Test
  @Order(5)
  def testCreateItemParquetFile(): Unit = {
    val fileLocation = parquetFilePath + "items.parquet/"
    val spark = createSparkSession()
    val itemRecords = dataGen.generateItemData(size)
    val itemRecordsRDD = spark.sparkContext.parallelize(itemRecords)

    createParquetFile(itemRecordsRDD, fileLocation)(
      spark,
      itemRankRowEncoder
    )
    val file = new java.io.File(fileLocation)
    assert(file.exists)
    spark.close()
  }

  @AfterAll
  def tearDown(): Unit = {
    val file = new java.io.File(parquetFilePath)
    deleteRecursively(file)
    assert(!file.exists)

  }
}
