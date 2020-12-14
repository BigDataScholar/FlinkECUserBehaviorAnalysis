package com.hypers.OrderTimeoutDetect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
 * @Author: Alice菌
 * @Date: 2020/12/13 15:57
 * @Description: 
    来自两条流的订单交易匹配  （ connect 实现 ）
 */
object OrderPayTxMatch {

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

    // connect 连接两条流，匹配事件进行处理
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptStream)
      .process(new OrderPayTxDetect())

    // 定义侧输出流
    val unmatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatched-receipts")

    // 打印输出
    resultStream.print("matched")
    resultStream.getSideOutput(unmatchedPays).print("unmatched-pays")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts")
    env.execute("order pay tx match job")

  }

  // 定义 CoProcessFunction，实现两条流数据的匹配检测
  class OrderPayTxDetect() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{

    // 定义两个 ValueState，保存当前交易对应的支付事件和到账事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

    //定义侧输出流
    val unmatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatched-receipts")

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // pay 来了，考察有没有对应的 receipt 来过
      val receipt: ReceiptEvent = receiptState.value()
      if (receipt != null){
        // 如果已经有 receipt，正常输出到主流
        out.collect((pay,receipt))
        receiptState.clear()
      }else{
        // 如果 receipt 还没来，那么把 pay 存入状态，注册一个定时器等待 5 秒
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //receipt来了，考察有没有对应的pay来过
      val pay: OrderEvent = payState.value()
      if (pay != null) {
        //如果已经有pay，那么正常匹配，输出到主流
        out.collect((pay, receipt))
        payState.clear()
      }else{
      // 如果 pay 还没来，那么把 receipt 存入状态，注册一个定时器等待 3 秒
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
    }
  }

    // 定时触发， 有两种情况，所以要判断当前有没有pay和receipt
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]
      #OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      //如果 pay 不为空，说明receipt没来，输出unmatchedPays
      if (payState.value() != null){
        ctx.output(unmatchedPays,payState.value())
      }

      if (receiptState.value() != null){
      ctx.output(unmatchedReceipts,receiptState.value())
      }

      // 清除状态
      payState.clear()
      receiptState.clear()
    }
    }
}
