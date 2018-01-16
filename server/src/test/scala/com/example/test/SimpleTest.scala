package com.example.test

import com.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec

import scala.collection.mutable.Stack
import scala.io.Source.fromFile

class SimpleTest extends FlatSpec with SharedSparkContext {

  val location = "server/src/test/resources/sampleData.csv"
  val sampleDf = spark.read.csv(location)

  "Sample df" should "be of length 6" in {
    assert(sampleDf.count==6)
  }

  it should "be of sameLength" in {
    val x = fromFile(location)
    val y = x.getLines.toList
    print(y)
    assert(sampleDf.count==y.length)
  }
}
