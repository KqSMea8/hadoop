package com.ganjunhua.spark.sparksql

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkLinkHive {
  def main(args: Array[String]): Unit = {
    val warehouseLocaltion =
      new File("spark-warehouse")
        .getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", warehouseLocaltion)
      .config("hive.metastore.uris", "thrift://holiday-1:9083")
      .enableHiveSupport()
      .getOrCreate()

    println("xxx")
    spark.sql("select * from default.record").show()
    spark.stop()
  }
}
