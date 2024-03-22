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

import util.Structures.{detectionRowEncoder, locationRowEncoder}

import java.io.File

object Utils { self =>
  val STRING_LENGTH = 500
  val NUM_PARTITIONS = 2
  val LOCATION_SIZE = 10000
  val DETECTION_SIZE = 1000000

  val detectionOidIndex: Int =
    detectionRowEncoder.schema.fieldIndex("detection_oid")
  val itemNameIndex: Int = detectionRowEncoder.schema.fieldIndex("item_name")
  val geographicalOidDetectionIndex: Int =
    detectionRowEncoder.schema.fieldIndex("geographical_location_oid")
  val geographicalOidLocationIndex: Int =
    locationRowEncoder.schema.fieldIndex("geographical_location_oid")
  val geographicalLocationNameIndex: Int =
    detectionRowEncoder.schema.length + locationRowEncoder.schema.fieldIndex(
      "geographical_location"
    ) - 1

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

  def validFilePath(filePath: String): Boolean = {
    val file = new File(filePath)
    if (file.exists && file.isFile && file.getName.endsWith(".parquet")) {
      true
    } else {
      throw new IllegalArgumentException(s"Invalid file path: $filePath")
    }
  }

  def isInt(s: String): Boolean = {
    try {
      s.toInt
      true
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"Invalid integer: $s")
    }
  }
}
