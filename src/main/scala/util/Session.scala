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

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Row, SaveMode, SparkSession}

object Session {
  def createSparkSession(): SparkSession = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("htx_spark_test")
      .set("spark.executor.memory", "4g") // Set executor memory
      .set("spark.driver.memory", "2g") // Set driver memory
      .set("spark.sql.shuffle.partitions", "4") // Set number of shuffle partitions

    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def createParquetFile(data: RDD[Row], path: String)(implicit
      spark: SparkSession,
      encoder: Encoder[Row]
  ): Unit = {
    import spark.implicits._
    val df = data.toDF()
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }

  def readParquetFile(
      path: String
  )(implicit spark: SparkSession): RDD[Row] = {
    spark.read.parquet(path).rdd
  }
}
