package com.ganjunhua.spark.sparksql

import org.apache.spark.sql.SparkSession

object SparkHiveToES {
  def main(args: Array[String]): Unit = {
    var setMaster = "local[*]"
    var setTableName = "edw.gjh"

    if (args.size == 2) {
      setMaster = args(0)
      setTableName = args(1)
    }
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master(setMaster)
      .getOrCreate()
    val hiveDF = spark.read
      .format("jdbc")
      .option("url", "jdbc://hive2://temp476.bigdata.lfk.360es.cn:10000")
      .option("dbtable", setTableName)
      .option("user", "root")
      .option("password", "123456")
      .load()
    hiveDF.printSchema()

  }
}
