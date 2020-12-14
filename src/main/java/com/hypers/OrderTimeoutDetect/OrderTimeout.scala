package com.hypers.OrderTimeoutDetect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/*
 * @Author: Alice菌
 * @Date: 2020/12/13 19:35
 * @Description: 
    
 */
object OrderTimeout {

  // 定义输入的订单事件样例类
  case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

  // 定义输出的订单检测结果样例类
  case class OrderResult(orderId: Long, resultMsg: String)

  def main(args: Array[String]): Unit = {
    // 定义流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置程序并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 读取输入的订单数据流
    val orderEventStream: DataStream[OrderEvent] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        // 示例数据： 34729,pay,sd76f87d6,1558430844
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
      })
      // 设置水印
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })

    // 自定义 Process Function,做精细化的流程控制
    val orderResultStream: DataStream[OrderResult] = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchDetect())

    // 打印输出
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")

    // 执行程序
    env.execute("order timeout without cep job")
  }

  class OrderPayMatchDetect() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
    // 定义状态，用来保存是否来过 create 和 pay 事件的标识位，以及定时器的时间戳
    lazy val isPayState:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
    lazy val isCreateState:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))

    // 定义一个状态，保存每次定时器的时间戳
    lazy val timerTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

    // 定义一个侧输出流
    val orderTimeOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

      // 取出当前的状态
      val isPayed: Boolean = isPayState.value()
      val isCreated: Boolean = isCreateState.value()
      val timeTs: Long = timerTsState.value()

      // 判断当前事件的类型，分成不同的情况讨论：
      // 情况1： 来的是 create，要继续判断之前是否有 pay 来过
      if (value.eventType == "create"){
        // 情况 1.1 ： 如果已经pay过，匹配成功，输出到主流，清空状态
        if (isPayed){
          out.collect(OrderResult(value.orderId,"payed successfully"))
          // 清除状态
          isPayState.clear()
          timerTsState.clear()
          // 删除定时器
          ctx.timerService().deleteEventTimeTimer(timeTs)
        }
          // 情况 1.2：如果没有pay过，那么就注册一个15分钟后的定时器，开始等待
        else{
          val ts: Long = value.eventTime * 1000L + 15 * 60 *1000L
          // 设置一个15分钟的定时器
          ctx.timerService().registerEventTimeTimer(ts)

          timerTsState.update(ts)
          isCreateState.update(true)
        }
      }

      // 情况2：来的是pay，要继续判断是否来过 create
      else if (value.eventType == "pay"){
        // 情况2.1 ： 如果 create 已经来过，匹配成功，要继续判断间隔时间是否超过了15分钟
        if (isCreated){
          // 情况 2.1.1：如果没有超时，正常输出结果到主流
          if (value.eventTime * 1000L < timeTs){
            out.collect(OrderResult(value.orderId,"payed successfully"))
          }else{
            // 情况2.1.2： 如果已经超时，那么输出 timeout 报警到侧输出流
            ctx.output(orderTimeOutputTag,OrderResult(value.orderId,"payed but already timeout"))
          }
          // 无论哪种情况，都已经有了输出，清空状态
          isCreateState.clear()
          timerTsState.clear()
          ctx.timerService().deleteEventTimeTimer(timeTs)
        }
           // 情况2.2 ：如果 create 没来，需要等待乱序 create，注册一个当前pay时间戳的定时器
        else{
          val ts: Long = value.eventTime * 1000L
          // 设置定时器
          ctx.timerService().registerEventTimeTimer(ts)
          // 更新状态
          timerTsState.update(ts)
          isPayState.update(true)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 定时器触发，需要判断是哪种情况
      if (isPayState.value()){
        // 如果 pay 过,那么说明 create没来，可能出现了数据丢失异常的情况
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"already payed but not found created log"))
      }else{
        // 如果 没有 pay过，那么说明真正 15 分钟 超时 [提交了订单，但是超过了15分钟仍未支付]
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"order timeout"))
      }

      // 清空状态
      isPayState.clear()
      isCreateState.clear()
      timerTsState.clear()

    }
  }
}
