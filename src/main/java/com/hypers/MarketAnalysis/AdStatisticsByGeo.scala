package com.hypers.MarketAnalysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/*
 * @Author: Alice菌
 * @Date: 2020/12/11 10:52
 * @Description: 
    页面广告点击量统计 (开一小时的时间窗口，滑动距离为5秒)
 */
object AdStatisticsByGeo {

  // 定义输入数据样例类
  case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
  // 定义输出数据样例类
  case class AdCountByProvince(province:String,windowEnd:String,count:Long)

  def main(args: Array[String]): Unit = {

    // 设置流处理的环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置程序的并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\AdClickLog.csv")
      .map(data => {
        // 样例数据：561558,3611281,guangdong,shenzhen,1511658120
        val dataArray: Array[String] = data.split(",")
        AdClickEvent(dataArray(0).toLong,dataArray(1).toLong,dataArray(2),dataArray(3),dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)    // 添加水印
      .keyBy(_.province)    // 按照 province 分组
      .timeWindow(Time.hours(1),Time.seconds(5))   // 设置窗口的大小为1h,滑动距离为5s
      .process(new AdCount)     // 开窗聚合统计
      .print()    // 输 出 结 果

    // 执行程序
    env.execute("ad analysis job")

  }

  class AdCount() extends ProcessWindowFunction[AdClickEvent,AdCountByProvince,String,TimeWindow]{

    override def process(key: String, context: Context, elements: Iterable[AdClickEvent], out: Collector[AdCountByProvince]): Unit = {

      // 因为我们是按照 province 进行分组
      // 所以这里直接根据 key 就能获取到 province
      val province: String = key
      // 将 窗口结束的时间戳 转换为 String 时间字符串
      val windowEnd: String = new Timestamp(context.window.getEnd).toString
      // 获取窗口元素的个数
      val count: Int = elements.size
      // 输出元素
      out.collect(AdCountByProvince(province,windowEnd,count))
    }
  }
}
