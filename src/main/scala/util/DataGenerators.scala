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

import org.apache.spark.sql.Row
import org.scalacheck.Gen
import util.Utils.STRING_LENGTH

import scala.util.Random

case class DataGenerators(location_size: Int, detection_size: Int) {
  private val locationOIDList = generateRandomLong(location_size)

  def generateDetectionData(cut: Int = 3): Seq[Row] = {
    if (detection_size <= cut) {
      throw new IllegalArgumentException(
        s"`cut` must be lesser than $detection_size"
      )
    }

    val skewLongValuesList = skewLongValues(detection_size)
    val detectionOIDList = generateRandomLong(detection_size - cut)
    val alphaNumList = generateAlphaNumString(detection_size - cut)
    val gen = for {
      geographical_location_oid <- Gen.oneOf(skewLongValuesList)
      video_camera_oid <- Gen.choose(Long.MinValue, Long.MaxValue)
      detection_oid <- Gen.oneOf(detectionOIDList)
      item_name <- Gen.oneOf(alphaNumList)
      timestamp_detected <- Gen.choose(Long.MinValue, Long.MaxValue)
    } yield Row(
      geographical_location_oid,
      video_camera_oid,
      detection_oid,
      item_name,
      timestamp_detected
    )

    (1 to detection_size).map(_ => gen.sample.get)
  }

  private def generateRandomLong(n: Int): List[Long] = {
    List.fill(n)(Random.nextLong)
  }

  private def skewLongValues(n: Int): List[Long] = {
    // divide by 2 but choose upper if odd
    val half = n / 2 + (n % 2)
    val halfRandomList = generateRandomLong(half)
    val halfStaticList = List.fill(n - half)(Long.MaxValue)
    halfRandomList ++ halfStaticList
  }

  private def generateAlphaNumString(n: Int): List[String] = {
    List.fill(n)(Random.alphanumeric.take(n).mkString)
  }

  def generateLocationData(): Seq[Row] = {
    val gen = for {
      geographical_location_oid <- Gen.oneOf(locationOIDList)
      geographical_location <- alphanumericString(STRING_LENGTH)
    } yield Row(geographical_location_oid, geographical_location)

    (1 to location_size).map(_ => gen.sample.get)
  }

  private def alphanumericString(length: Int): Gen[String] =
    Gen.listOfN(length, Gen.alphaNumChar).map(_.mkString)

  def generateItemData(numRows: Int): Seq[Row] = {
    val gen = for {
      geographical_location <- Gen.choose(Long.MinValue, Long.MaxValue)
      item_rank <- alphanumericString(STRING_LENGTH)
      item_name <- alphanumericString(STRING_LENGTH)
    } yield Row(geographical_location, item_rank, item_name)

    (1 to numRows).map(_ => gen.sample.get)
  }
}
