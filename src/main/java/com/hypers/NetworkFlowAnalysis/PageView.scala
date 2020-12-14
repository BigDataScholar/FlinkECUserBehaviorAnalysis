package com.hypers.NetworkFlowAnalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * @Author: Alice菌
 * @Date: 2020/12/5 10:36
 * @Description:
   实时统计每小时内的网站PV
 */
object PageView {

  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  def main(args: Array[String]): Unit = {

    // 创建 流处理的 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义为 eventTime -- 事件创建的时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置程序的并行度
    env.setParallelism(1)

    // 读取文本数据
    env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\UserBehavior.csv")
      // 对文本数据进行封装处理
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        // 将数据封装进 UserBehavior
        UserBehavior(dataArray(0).toLong,dataArray(1).toLong,dataArray(2).toInt,dataArray(3),dataArray(4).toLong)
      })
      // 设置水印
      .assignAscendingTimestamps(_.timestamp * 1000)
      // 过滤出 "pv" 数据
      .filter(_.behavior == "pv")
      // 求和
      .map(x => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60 * 60))
      .sum(1)
      .print()

    // 执行程序
    env.execute("Page View Job")

  }
}
