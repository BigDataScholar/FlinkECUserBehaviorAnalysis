package com.hypers.MarketAnalysis


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/*
 * @Author: Alice菌
 * @Date: 2020/12/11 11:37
 * @Description:
    黑名单过滤
 */
object AdAnalysisByProvinceBlack {

  // 定义输入输出样例类
  case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
  case class AdCountByProvince(province:String,windowEnd:String,count:Long)

  //定义侧输出流报警信息样例类
  case class BlackListWarning(userId:Long,adId:Long,msg:String)

  def main(args: Array[String]): Unit = {

    // 定义流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val adLogStream: DataStream[AdClickEvent] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\AdClickLog.csv")
      .map(data => {
        // 样例数据：561558,3611281,guangdong,shenzhen,1511658120
        val dataArray: Array[String] = data.split(",")
        AdClickEvent(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L )   // 设置水印

    //定义刷单行为 过滤操作
    val filterBlackListStream: DataStream[AdClickEvent] = adLogStream // 设置水印
      .keyBy(data =>(data.userId, data.adId)) // 按照用户 和 广告id进行分组)
      .process(new FilterBlackList(100L))

    // 按照 province分组开窗聚合统计
    val adCountStream: DataStream[AdCountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5)) // 设置窗口大小为1h , 滑动距离为5s
      .aggregate(new AdCountAgg(), new AdCountResult())

    // 打印结果
    adCountStream.print()
    // 打印测输出流的数据
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("blacklist")

    // 执行程序
    env.execute("as analysis job")

  }

  // 实现自定义 ProcessFunction
  class FilterBlackList(maxClickCount:Long) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{

    // 定义一个状态，需要保存当前用户对当前广告的点击量 count
    lazy val countState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
    // 定义一个标识位，用来表示用户是否已经在黑名单中
    lazy val isSendState:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent",classOf[Boolean]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 取出状态数据
      val curCount: Long = countState.value()

      // 如果是第一个数据，那么注册第二天0点的定时器，用于清空状态
      if (curCount == 0){
        val ts: Long = (ctx.timerService().currentProcessingTime() / (1000*60*60*24) + 1) * (1000*60*60*24)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }
      // 判断 count 值是否达到上限，如果达到，并且之前没有输出过报警信息，那么则报警
      if (curCount > maxClickCount){
        if (!isSendState.value()){
          // 侧输出数据
          ctx.output(new OutputTag[BlackListWarning]("blacklist"),BlackListWarning(value.userId,value.adId,"click over"+maxClickCount+"times today"))
          // 更新黑名单状态
          isSendState.update(true)
        }
        // 如果达到上限，则不再进行后续的操作，即此后其点击行为不应该再统计
        return
      }

      // count 值 + 1
      countState.update(curCount + 1)
      // 输出数据
      out.collect(value)

    }

    // 0 点触发定时器，直接清空状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      countState.clear()
      isSendState.clear()
    }
  }

  // 自定义预聚合函数
  class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  // 自定义窗口函数，第一个参数就是预聚合函数最后输出的值,Long
  class AdCountResult() extends WindowFunction[Long,AdCountByProvince,String,TimeWindow]{

    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {

      out.collect(AdCountByProvince(key,new Timestamp(window.getEnd).toString,input.head))
    }
  }

}
