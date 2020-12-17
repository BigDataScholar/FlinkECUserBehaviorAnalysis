package com.hypers.HotItemsAnalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
 * @Author: Alice菌
 * @Date: 2020/11/23 10:38
 * @Description:
      电商用户行为数据分析： 热门商品实时统计
      <  每隔5分钟输出最近一小时内点击量最多的前N个商品   >
 */
object HotItems {

  // 定义样例类，用于封装数据
  case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timeStamp:Long)
  // 中间输出的商品浏览量的样例类
  case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

  def main(args: Array[String]): Unit = {

    // 定义流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 读取文本文件
    val stream: DataStream[String] = env.readTextFile("G:\\idea arc\\BIGDATA\\project\\src\\main\\resources\\UserBehavior.csv")
    // 对读取到的数据源进行处理
    stream.map(data =>{
      val dataArray: Array[String] = data.split(",")
      // 将数据封装到新建的样例类中
      UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
    })
      // 设置waterMark(水印)  --  处理乱序数据
      .assignAscendingTimestamps(_.timeStamp * 1000)
      // 过滤出 “pv” 的数据  -- 过滤出点击行为数据
      .filter(_.behavior == "pv")
      // 因为需要统计出每种商品的个数,这里先对商品id进行分组
      .keyBy(_.itemId)
      // 需求: 统计近1小时内的热门商品，每5分钟更新一次  -- 滑动窗口聚合
      .timeWindow(Time.hours(1),Time.minutes(5))
      // 预计算，统计出每种商品的个数
      .aggregate(new CountAgg(),new WindowResult())
      // 按每个窗口聚合
      .keyBy(_.windowEnd)
      // 输出每个窗口中点击量前N名的商品
      .process(new TopNHotItems(3))
      .print("HotItems")
    
    // 执行程序
    env.execute("HotItems")

  }

  // 自定义预聚合函数，来一个数据就加一
  class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{

    // 定义累加器的初始值
    override def createAccumulator(): Long = 0L

    // 定义累加规则
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    // 定义得到的结果
    override def getResult(accumulator: Long): Long = accumulator

    // 合并的规则
    override def merge(a: Long, b: Long): Long = a + b

  }

  /**
   * WindowFunction [输入参数类型，输出参数类型，Key值类型，窗口类型]
   * 来处理窗口中的每一个元素(可能是分组的)
   */
  // 自定义窗口函数，包装成 ItemViewCount输出
  class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow] {

    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

      // 在前面的步骤中，我们根据商品 id 进行了分组，次数的 「 key 」 就是  商品编号
      val itemId: Long = key
      // 获取 窗口 末尾
      val windowEnd: Long = window.getEnd
      // 获取点击数大小 【累加器统计的结果】
      val count: Long = input.iterator.next()

      // 将获取到的结果进行上传
      out.collect(ItemViewCount(itemId,windowEnd,count))
    }
  }

  // 自定义 process function，排序处理数据
  class TopNHotItems(nSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {

    // 定义一个状态变量 list state，用来保存所有的 ItemViewCont
    private var itemState: ListState[ItemViewCount] = _

    // 在执行processElement方法之前，会最先执行并且只执行一次 open 方法
    override def open(parameters: Configuration): Unit = {
      // 初始化状态变量
      itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount]))
    }

    // 每个元素都会执行这个方法
    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每一条数据都存入 state 中
      itemState.add(value)
      // 注册 windowEnd+1 的 EventTime Timer, 延迟触发，当触发时，说明收齐了属于windowEnd窗口的所有商品数据，统一排序处理
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    // 定时器触发时，会执行 onTimer 任务
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      // 已经收集到所有的数据，首先把所有的数据放到一个 List 中
      val allItems: ListBuffer[ItemViewCount] = new ListBuffer()

      import scala.collection.JavaConversions._

      for (item <- itemState.get()) {
        allItems += item
      }

      // 将状态清除
      itemState.clear()

      // 按照 count 大小  倒序排序
      val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(nSize)

      // 将数据排名信息格式化成 String，方便打印输出
      val result: StringBuilder = new StringBuilder()
      result.append("======================================================\n")
      // 触发定时器时，我们多设置了0.1秒的延迟，这里我们将时间减去0.1获取到最精确的时间
      result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")

      // 每一个商品信息输出 (indices方法获取索引)
      for( i <- sortedItems.indices){
        val currentTtem: ItemViewCount = sortedItems(i)
        result.append("No").append(i + 1).append(":")
          .append("商品ID=").append(currentTtem.itemId).append("  ")
          .append("浏览量=").append(currentTtem.count).append("  ")
          .append("\n")
      }
     
      result.append("======================================================\n")

      // 设置休眠时间
      Thread.sleep(1000)
      // 收集数据
      out.collect(result.toString())
    }
  }

}

