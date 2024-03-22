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

import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import util.Session.{createParquetFile, createSparkSession}
import util.Structures.locationRowEncoder
import util.Utils.{DETECTION_SIZE, LOCATION_SIZE, deleteRecursively, isInt}

@TestMethodOrder(classOf[OrderAnnotation])
@TestInstance(Lifecycle.PER_CLASS)
class UtilsTest {
  private val parquetFilePath: String = "src/test/resources/"
  private val dataGen = DataGenerators(LOCATION_SIZE, DETECTION_SIZE)
  private val fileLocation = parquetFilePath + "items.parquet/"

  @BeforeAll
  def setUp(): Unit = {
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
  @Order(1)
  def testValidFilePath(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Utils.validFilePath(fileLocation)
      }
    )

  }

  @Test
  @Order(2)
  def testDeleteRecursively(): Unit = {
    val file = new java.io.File(parquetFilePath)
    deleteRecursively(file)
    assert(!file.exists)
  }

  @Test
  @Order(3)
  def testIsInteger(): Unit = {

    assertThrows(
      classOf[IllegalArgumentException],
      () => {
        isInt("abc")
      }
    )

    assertTrue(isInt("123"))
  }
}
