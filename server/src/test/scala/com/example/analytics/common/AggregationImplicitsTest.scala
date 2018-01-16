package com.example.analytics.common

import com.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, FunSuite}
import scala.io.Source._
import com.example.analytics.common.AggregationImplicits._

class AggregationImplicitsTest extends FlatSpec with SharedSparkContext{

  val location = "server/src/test/resources/sampleData.csv"
  val sampleDf = spark.read.csv(location)

  "Sample df" should "be of length 6" in {
    val x = sampleDf.getAggFromListIndexes[String](Some(List(1,2)),None)
    println(x)
//    println("df partitions :: " +sampleDf.rdd.partitions.length)
    println("df columnNames :: " +sampleDf.columns)
    print(x.get(1))
    assert(x.isSuccess)
  }


}
