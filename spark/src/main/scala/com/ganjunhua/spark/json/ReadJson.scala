package com.ganjunhua.spark.json

import java.io.FileInputStream

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession


object ReadJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //file:///D:\Holiday\App\Git\hadoop\spark\data\json\test.json
    val path = "data/json"
    val jsonRDD1 = spark.sparkContext
      .textFile("data/json/test.json")
    val dataRDD1 = jsonRDD1.map(json => {
      val jsonObject = JSON.parseObject(json)
      val name = jsonObject.getOrDefault("name", null)
      val age = jsonObject.getOrDefault("age", null)
      (name, age)
    })
    dataRDD1.foreach(println)
    spark.stop()
  }
}