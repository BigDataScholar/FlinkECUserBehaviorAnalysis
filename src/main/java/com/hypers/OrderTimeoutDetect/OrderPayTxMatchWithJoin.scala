package com.hypers.OrderTimeoutDetect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
 * @Author: Alice菌
 * @Date: 2020/12/12 20:23
 * @Description: 
     来自两条流的订单交易匹配  （  JOIN 实现 ）
 */
object OrderPayTxMatchWithJoin {

  // 输入输出的样例类
  case class ReceiptEvent(txId:String, payChannel:String, timestamp:Long)
  case class OrderEvent(orderId:Long, eventType:String, txId:String, eventTime:Long)

  def main(args: Array[String]): Unit = {
    // 创建流处理的环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置程序并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从 OrderLog.csv 文件中读取数据 ，并转换成样例类
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        // 样例数据 ：  34731,pay,35jue34we,1558430849
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).toLong,dataArray(1),dataArray(2),dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })       //  为数据流中的元素分配时间戳
      .filter(_.eventType != "") // 只过滤出pay事件
      .keyBy(_.txId)    // 根据 订单id 分组

    // 从 ReceiptLog.csv 文件中读取数据 ，并转换成样例类
    val receiptStream: KeyedStream[ReceiptEvent, String] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\ReceiptLog.csv")
      .map(data => {
        // 样例数据： 3hu3k2432,alipay,1558430848
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 设置水印
      .keyBy(_.txId)   // 根据 txId 进行分组

    // 使用 join 连接两条流
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .intervalJoin(receiptStream)
      .between(Time.seconds(-5), Time.seconds(3))    // join范围 第二条流 时间范围 在第一条流 -5 < x < 3 范围内
      .process(new OrderPayTxDetectWithJoin())

    resultStream.print()
    env.execute("order pay tx match with join job")

  }

  // 自定义 ProcessJoinFunction
  class OrderPayTxDetectWithJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      collector.collect((left,right))
    }
  }
}
