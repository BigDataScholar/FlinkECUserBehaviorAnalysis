package com.hypers.NetworkFlowAnalysis

import java.lang
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


/*
 * @Author: Alice菌
 * @Date: 2020/12/5 15:45
 * @Description: 
    使用布隆过滤器的UV统计
 */

object UvWithBloomFilter {

  // 定义样例类，用于封装数据
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  case class UvCount(windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {

    // 创建 流处理的 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义为 eventTime -- 事件创建的时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置程序的并行度
    env.setParallelism(1)

    // 读取文本数据
    env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\UserBehavior.csv")
      // 对文本数据进行封装处理
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        // 将数据封装进 UserBehavior
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      // 设置水印 [ 升序时间戳 ]
      .assignAscendingTimestamps(_.timestamp * 1000)
      // 只统计 "pv" 数据
      .filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      // 设置窗口大小为一个小时
      .timeWindow(Time.hours(1))
      // 我们不应该等待窗口关闭才去做 Redis 的连接 -》 数据量可能很大，窗口的内存放不下
      // 所以这里使用了 触发窗口操作的API -- 触发器 trigger
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())
      .print()

    // 执行程序
    env.execute("uv with bloom Job")

  }

  // 自定义窗口触发器
  class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    // 如果事件是基于 processTime 触发
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
       
      TriggerResult.CONTINUE
    }

    // 如果事件是基于 eventTime 触发
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

      TriggerResult.CONTINUE
    }

    // 收尾工作
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

    // 每来一个元素就触发
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

      // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
      TriggerResult.FIRE_AND_PURGE

    }
  }


  // 定义一个布隆过滤器
  class Bloom(size: Long) extends Serializable {
    // 位图的总大小
    private val cap = if (size > 0) size else 1 << 27

    // 定义 hash 函数
    def hash(value: String, seed: Int) = {

      var result: Long = 0L
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt(i)
      }
      result & (cap - 1)
    }
  }

  // 自定义窗口处理函数
  class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

    // 创建 redis 连接
    lazy val jedis = new Jedis("node02", 6379)

    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
      // 位图的存储方式， key 是 windowEnd，value 是 bitmap
      val storeKey: String = context.window.getEnd.toString
      var count = 0L
      // 把每个窗口的 uv count 值也存入 redis 表，存放内容为(windowEnd > uvCount)，所以要先从 redis 中读取
      if (jedis.hget("count", storeKey) != null) {
        count = jedis.hget("count", storeKey).toLong
      }

      // 用 布隆过滤器 判断当前用户是否已经存在
      // 因为是每来一条数据就判断一次，所以我们就可以直接用last获取到这条数据
      val userId: String = elements.last._2.toString
      // 计算哈希
      val offset: Long = bloom.hash(userId, 61)
      // 定义一个标志位，判断 redis 位图中有没有这一位
      val isExist: lang.Boolean = jedis.getbit(storeKey, offset)

      if (!isExist) {
        // 如果不存在，位图对应位置1，count + 1
        jedis.setbit(storeKey, offset, true)
        jedis.hset("count", storeKey, (count + 1).toString)
        out.collect(UvCount(storeKey.toLong, count + 1))
      } else {
        // 输出到 flink
        out.collect(UvCount(storeKey.toLong, count))
      }
    }
  }

}

