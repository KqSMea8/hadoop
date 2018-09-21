package com.ganjunhua.spark.sparkstreaming

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object MapWithStateCheckPoint {
  var checkPointPath: String = "D:" + File.separator + "spark" + File.separator + "checkPointPath"
  var streamSeconds = 10
  var dataPath = "D:" + File.separator + "spark"
  var setAppName = this.getClass.getSimpleName
  var setMaster = "local[*]"

  def main(args: Array[String]): Unit = {
    if (args.size == 5) {
      checkPointPath = args(0)
      streamSeconds = args(1).toInt
      dataPath = args(2)
      setAppName = args(3)
      setMaster = args(4)
    } else if (args.size > 5) {
      println("参数错误")
      return
    }
    val ssc = StreamingContext.getOrCreate(checkPointPath, functionCreateSCC)
    ssc.start()
    ssc.awaitTermination()
  }

  def functionCreateSCC(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(setAppName)
      .setMaster(setMaster)
    val ssc = new StreamingContext(conf, Seconds(streamSeconds))
    ssc.checkpoint(checkPointPath)
    val initData = ssc.sparkContext.parallelize(List[(String, Int)]())
    val line = ssc.textFileStream(dataPath)
    val splitWords = line.flatMap(x => x.split(",")).map(x => (x, 1))
    val wordCount = splitWords.mapWithState(StateSpec.function(newMapWithState).initialState(initData))
    wordCount.print()
    ssc
  }

  def newMapWithState = (word: String, one: Option[Int], state: State[Int]) => {
    // 现有值 + 历史值
    val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
    //返回叠加值
    val outPut = (word, sum)
    // 更新历史值
    state.update(sum)
    outPut
  }
}
