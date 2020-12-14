package com.hypers.NetworkFlowAnalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * @Author: Alice菌
 * @Date: 2020/12/5 14:54
 * @Description: 
    
 */
object UniqueVisitor {

  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
  case class UvCount(windowEnd: Long, count: Long)

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
      // 只统计 "pv" 数据
      .filter(_.behavior == "pv")
      // 设置窗口大小为一个小时
      .timeWindowAll(Time.seconds(60 * 60))
      .apply(new UvCountByWindow())
      .print()

    // 执行程序
    env.execute("Page View Job")
  }

  class UvCountByWindow extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{

    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

      // 定义一个 scala set，用于保存所有的数据 userId 并去重
      var idSet: Set[Long] = Set[Long]()
      // 把当前窗口所有数据的ID收集到 set 中，最后输出 set 的大小
      for ( userBehavior <- input){
        idSet += userBehavior.userId
      }
      // 输出结果
      out.collect(UvCount(window.getEnd,idSet.size))

    }
  }
}
