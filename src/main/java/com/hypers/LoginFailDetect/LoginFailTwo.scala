package com.hypers.LoginFailDetect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
 * @Author: Alice菌
 * @Date: 2020/11/23 16:51
 * @Description: 
    
 */
object LoginFailTwo {

  // 输入的登录事件样例类
  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

  // 输出的报警信息样例类
  case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

  def main(args: Array[String]): Unit = {

    // 创建流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 构建数据
    env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\LoginLog.csv")
      .map(data => {
        // 将文件中的数据封装成样例类
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      // 设置水印
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
    })
      // 以用户id为key，进行分组
      .keyBy(_.userId)
      // 自定义处理函数
      .process(new LoginWarning(2))
      .print()

    //  执行程序
    env.execute("login fail job")


  }

  // 自定义处理函数，保留上一次登录失败的事件    [键的类型，输入元素的类型，输出元素的类型]
  class LoginWarning(maxFailTimes:Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    // 定义  保存登录失败事件的状态
    lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-state", classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
      // 首先按照type做筛选，如果success直接清空，如果fail再做处理
      if(value.eventType == "fail"){
        // 先获取之前失败的事件
        val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()
        if (iter.hasNext){
          // 如果之前已经有失败的事件，就做判断，如果没有就把当前失败事件保存进state
          val firstFailEvent: LoginEvent = iter.next()
          // 判断两次失败事件间隔小于2秒，输出报警信息
          if (value.eventTime < firstFailEvent.eventTime + 2){
            out.collect(Warning( value.userId,firstFailEvent.eventTime,value.eventTime,"在2秒内连续两次登录失败。"))
          }

          // 更新最近一次的登录失败事件，保存在状态里
          loginFailState.clear()
          loginFailState.add(value)

        }else{
          // 如果是第一次登录失败，之前把当前记录 保存至 state
          loginFailState.add(value)
        }
      }else{
        // 当前登录状态 不为 fail，则直接清除状态
        loginFailState.clear()
      }
    }
  }

}
