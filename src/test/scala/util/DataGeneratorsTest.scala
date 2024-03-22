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

package util;

import org.junit.jupiter.api.Test
import util.Structures.detectionRowEncoder

class DataGeneratorsTest {
  private val item_size = 10
  private val location_size = 100
  private val detection_size = 1000
  private val dataGen = DataGenerators(location_size, detection_size)

  @Test
  def testGenerateLocationRecords(): Unit = {
    val locationRecords = dataGen.generateLocationData()
    assert(locationRecords.length == location_size)
  }

  @Test
  def testGenerateDetectionRecords(): Unit = {
    val detectionRecords = dataGen.generateDetectionData()
    assert(detectionRecords.length == detection_size)
    val col_index = detectionRowEncoder.schema.fieldIndex("detection_oid")
    val detectionOids = detectionRecords.map(_.get(col_index))
    assert(detectionOids.distinct.length <= detectionOids.length)
  }

  @Test
  def testGenerateItemRecords(): Unit = {
    val itemRecords = dataGen.generateItemData(item_size)
    assert(itemRecords.length == item_size)
  }
}
