package com.hypers.MarketAnalysis


import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/*
 * @Author: Alice菌
 * @Date: 2020/12/7 17:32
 * @Description: 
    电商用户行为数据分析：  市场营销商业指标统计分析
    APP市场推广统计   - - > 分渠道统计
 */
object AppMarketingByChannel {

  // 定义一个输入数据的样例类  保存电商用户行为的样例类
  case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

  // 定义一个输出结果的样例类   保存 市场用户点击次数
  case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

  // 自定义数据源
  class SimulateEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

    // 定义是否运行的标识符
    var running: Boolean = true
    // 定义渠道的集合
    val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
    // 定义用户行为的集合
    val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
    // 定义随机数发生器
    val rand: Random.type = Random

    // 重写 run 方法
    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {

      // 获取到 Long类型的最大值
      val maxElements: Long = Long.MaxValue
      // 设置初始值
      var count: Long = 0L

      // 随机生成所有数据
      while (running && count < maxElements) {

        // 生成一个随机数
        val id: String = UUID.randomUUID().toString
        // 获取随机行为
        val behaviorType: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
        // 获取随机渠道
        val channel: String = channelSet(rand.nextInt(channelSet.size))
        // 获取到当前的系统时间
        val ts: Long = System.currentTimeMillis()
        // 输出生成的用户行为的事件流
        ctx.collect(MarketingUserBehavior(id, behaviorType, channel, ts))
        // count + 1
        count += 1
        // 设置休眠的时间
        TimeUnit.MICROSECONDS.sleep(10L)

      }
    }

    override def cancel(): Unit = running = false
  }

  def main(args: Array[String]): Unit = {

    // 创建流处理的环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.addSource(new SimulateEventSource()) //  添加数据源
      .assignAscendingTimestamps(_.timestamp) // 设置水印
      .filter(_.behavior != "UNINSTALL") // 过滤掉 卸载 的数据
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1) //以渠道和行为作为key分组
      .timeWindow(Time.hours(1), Time.seconds(1)) // 设置滑动窗口,窗口大小为1h,滑动距离为1s
      .process(new MarketingCountByChannel) // 调用自定义处理方法
      .print() // 输出结果

    // 执行程序
    env.execute("app marketing by channel job")

  }

  // 自定义处理函数
  class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {

    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {

      // 根据 context 对象分别获取到 Long 类型的 窗口的开始和结束时间
      //context.window.getStart是长整形   所以new 一个 变成String类型
      val startTs: String = new Timestamp(context.window.getStart).toString
      val endTs: String = new Timestamp(context.window.getEnd).toString

      // 获取到 渠道
      val channel: String = key._1
      // 获取到 行为
      val behaviorType: String = key._2
      // 获取到 次数
      val count: Int = elements.size

      // 输出结果
      out.collect(MarketingViewCount(startTs, endTs, channel, behaviorType, count))

    }
  }

}



