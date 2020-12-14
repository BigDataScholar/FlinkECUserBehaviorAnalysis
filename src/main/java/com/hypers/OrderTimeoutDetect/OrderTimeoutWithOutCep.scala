package com.hypers.OrderTimeoutDetect

import java.util

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
/*
 * @Author: Alice菌
 * @Date: 2020/12/11 15:46
 * @Description:

 */
object OrderTimeoutWithOutCep {

  // 定义输入的订单事件样例类
  case class OrderEvent(orderId:Long,eventType:String,eventTime:Long)
  // 定义输出的订单检测结果样例类
  case class OrderResult(orderId:Long,resultMsg:String)

  def main(args: Array[String]): Unit = {

    // 定义流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置程序并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 从文件中读取数据，并转换成样例类
    val orderEventStream: DataStream[OrderEvent] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        // 样例数据： 34729,pay,sd76f87d6,1558430844
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
      })  //  处理数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })  //  设置时间戳

    // 1、 定义一个匹配事件序列的模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")   // 首先是订单的 create 事件
      .followedBy("pay").where(_.eventType == "pay")  // 后面来的是订单的 pay 事件
      .within(Time.minutes(15))    // 间隔 15 分钟

    // 2、 将 pattern 应用在按照 orderId分组的数据流上
    val patterStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    // 3、定义一个侧输出流标签，用来标明超时事件的侧输出流
    val orderTimeOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order time out")

    // 4、调用select方法，提取匹配事件和超时事件，分别进行处理转换输出
    val result: DataStream[OrderResult] = patterStream
      .select(orderTimeOutputTag, new OrderTimeOutSelect(), new OrderPaySelect())

    // 5、打印输出
    result.print("payed")
    result.getSideOutput(orderTimeOutputTag).print("timeout")

    // 执行程序
    env.execute("order timeout detect job")

  }

  // 自定义超时处理函数
  class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
    override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
      val timeOutOrderId: Long = pattern.get("create").iterator().next().orderId
      OrderResult(timeOutOrderId,"timeout at" + timeoutTimestamp)
    }
  }

  // 自定义匹配处理函数
  class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
    override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {

      val payedOrderId: Long = pattern.get("pay").get(0).orderId
      OrderResult(payedOrderId,"pay successfully")
    }
  }
}
