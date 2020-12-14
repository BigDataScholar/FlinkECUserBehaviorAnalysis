package com.hypers.LoginFailDetect

import java.util

import com.hypers.LoginFailDetect.LoginFailOne.LoginEvent
import com.hypers.LoginFailDetect.LoginFailTwo.LoginEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
 * @Author: Alice菌
 * @Date: 2020/11/23 17:12
 * @Description: 
    
 */
object LoginFailWithCep {

  // 输入的登录事件样例类
  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

  // 输出的报警信息样例类
  case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

  def main(args: Array[String]): Unit = {

    // 1、创建流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 构建数据
    val loginEventStream: KeyedStream[LoginEvent, Long] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\LoginLog.csv")
      .map(data => {
        // 将文件中的数据封装成样例类
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      // 设置水印
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
      })
      // 以用户id为key，进行分组
      .keyBy(_.userId)

    // 定义匹配的模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))    // 通过 within 关键字定义了检测窗口时间时间

    // 将 pattern 应用到 输入流 上，得到一个 pattern stream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream,loginFailPattern)

    // 用 select 方法检出 符合模式的事件序列
    val loginFailDataStream: DataStream[Warning] = patternStream.select(new LoginFailMatch())

    // 将匹配到的符合条件的事件打印出来
    loginFailDataStream.print("warning")

    // 执行程序
    env.execute("login fail with cep job")

  }

  // 自定义 pattern select function
  // 当检测到定义好的模式序列时就会调用，输出报警信息
  class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{

    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
      // 从 map 中可以按照模式的名称提取对应的登录失败事件
      val firstFail: LoginEvent = map.get("begin").iterator().next()
      val secondFail: LoginEvent = map.get("next").iterator().next()

      Warning( firstFail.userId,firstFail.eventTime,secondFail.eventTime,"在2秒内连续2次登录失败。")

    }
  }
}
