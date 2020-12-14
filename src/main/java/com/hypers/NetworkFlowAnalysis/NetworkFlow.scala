package com.hypers.NetworkFlowAnalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scala.collection.mutable.ListBuffer

/*
 * @Author: Alice菌
 * @Date: 2020/11/23 14:16
 * @Description:
    电商用户行为数据分析：实时流量统计
    <每隔5秒，输出最近10分钟内访问量最多的前N个URL>
 */
object NetworkFlow {

  // 输入 log 数据样例类
  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

  // 中间统计结果样例类
  case class UrlViewCount(url: String, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {

    // 创建 流处理的 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义为 eventTime -- 事件创建的时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置任务并行度
    env.setParallelism(1)
    // 读取文件数据
    val stream: DataStream[String] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\apache.log")

    // 对 stream 数据进行处理
    stream.map(data => {
      val dataArray: Array[String] = data.split(" ")
      // 因为日志文件中的数据格式是  17/05/2015:10:05:03
      // 所以我们这里用DataFormat对时间进行转换
      val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp: Long = simpleDateFormat.parse(dataArray(3).trim).getTime
      // 将解析的数据存放至我们定义好的样例类中
      ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      // 因为我们需要统计出每种url的出现的次数，故这里将 url 作为 key 进行分组
      .keyBy(_.url)
      // 滑动窗口聚合   -- 每隔5秒，输出最近10分钟内访问量最多的前N个URL
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 预计算，统计出每个 URL 的访问量
      .aggregate(new CountAgg(), new WindowResult())
      // 根据窗口结束时间进行分组
      .keyBy(_.windowEnd)
      // 输出每个窗口中访问量最多的前5个URL
      .process(new TopNHotUrls(5)) //
      .print()


    //  执行程序
    env.execute("network flow job")

  }

  // 自定义的预聚合函数
  class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b

  }

  // 自定义的窗口处理函数
  class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {

    override def apply(url: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      // 输出结果
      out.collect(UrlViewCount(url, window.getEnd, input.iterator.next()))
    }
  }

  // 自定义 process function，实现排序输出
  class TopNHotUrls(nSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    // 定义一个状态列表，保存结果
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

      // 将数据添加至 状态列表中
      urlState.add(value)
      // 根据窗口结束时间windowEnd，设置定时器
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      // 新建一个ListBuffer，用于存放状态列表中的数据
      val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
      // 获取到状态列表
      val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()

      while (iter.hasNext) {
        allUrlViews += iter.next()
      }

      // 清除状态
      urlState.clear()

      // 按照 count 大小排序
      val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(nSize)

      // 格式化成String打印输出
      val result: StringBuilder = new StringBuilder()

      result.append("=========================================\n")
      // 触发定时器时，我们设置了一个延迟时间，这里我们减去延迟
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // 拼接打印结果
        result.append("No").append(i + 1).append(":")
          .append("  URL=").append(currentUrlView.url).append(" ")
          .append("  流量=").append(currentUrlView.count).append("\n")

      }

      result.append("=========================================\n")

      // 设置休眠时间
      Thread.sleep(1000)

      // 输出结果
      out.collect(result.toString())

    }
  }

}
