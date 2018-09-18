package com.ganjunhua.spark.jdbc

import org.apache.spark.sql.SparkSession

object HiveJdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val hiveDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:hive2://holiday-1:10000")
      .option("dbtable", "default.test")
      .option("user", "root")
      .option("password", "admin123")
      .load()

    hiveDF.show()
    hiveDF.printSchema()
    hiveDF.createOrReplaceTempView("test")
    spark.sqlContext.sql("select * from test").collect().foreach(println)
    spark.stop()
  }
}
