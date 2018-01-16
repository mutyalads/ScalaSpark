package com.spark.testing

  import java.util.Date
  import org.apache.spark.sql._
  import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkContextextends extends BeforeAndAfterAll { self: Suite =>

  val appID = new Date().toString + math
  .floor(math.random * 10E4)
  .toLong
  .toString

  implicit val spark = SparkSession
  .builder()
  .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
  .master("local")
  .appName(appID)
  .getOrCreate()

  override protected def afterAll(): Unit = {
  try {
  spark.stop()
} finally {
  super.afterAll()
}
}

}
