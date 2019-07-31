package com.yhs.flinkproject.gmall

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
// import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment} 替换成下面的
import org.apache.flink.streaming.api.scala._

/*
我们将实现一个“实时热门商品”的需求，
可以将“实时热门商品”翻译成程序员更好理解的需求：每隔5分钟输出最近一小时内点击量最多的前N个商品。
将这个需求进行分解我们大概要做这么几件事情：
1.抽取出业务时间戳，告诉Flink框架基于业务时间做窗口
2.过滤出点击行为数据
3.按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
4.按每个窗口聚合，
5.输出每个窗口中点击量前N名的商品
 */
object HotItems {
  // 数据源存放的路径 全路径
  var filePath: String = "C:\\Develop\\IdeaWorkSpace\\gmall-userbehavioranalysis\\datasource\\UserBehavior.csv"

  def main(args: Array[String]): Unit = {
    // 0.获取运行时环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 从调用时刻开始给env创建的每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置为业务时间为事件时间
    env.setParallelism(1) // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响

    // 1.获取数据源

    /** 数据格式  .csv文件
      * 543462,1715,1464116,pv,1511658000
      * 662867,2244074,1575622,pv,1511658000
      */
    val dataSource: DataStream[String] = env.readTextFile(filePath)

    // 2.过滤出我们想要的数据,并封装成对象
    // 先试用filter过滤掉一部分数据， 防止数据集过大，也可以先封住对象，在进行过滤
    val filterDataSource: DataStream[String] = dataSource.filter(_.contains("pv"))
    // todo : 采坑-->用的是scala中的隐式转换，import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}==》import org.apache.flink.streaming.api.scala._
    val userBehaviorDataStream: DataStream[UserBehavior] = filterDataSource.map {
      line => {
        val fields: Array[String] = line.split(",")
        UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
      }
    }

    /**
      * todo :这里注意，我们需要统计业务时间上的每小时的点击量，所以要基于EventTime来处理。那么如果让Flink按照我们想要的业务时间来处理呢？这里主要有两件事情要做。
      * todo :第一件是告诉Flink我们现在按照EventTime模式进行处理，Flink默认使用ProcessingTime处理，
      * todo :第二件事情是指定如何获得业务时间，以及生成Watermark。Watermark是用来追踪业务事件的概念，
      * todo :可以理解成EventTime世界中的时钟，用来指示当前处理到什么时刻的数据了。由于我们的数据源的数据已经经过整理，
      * todo :没有乱序，即事件的时间戳是单调递增的，所以可以将每条数据的业务时间就当做Watermark。
      * todo :这里我们用assignAscendingTimestamps来实现时间戳的抽取和Watermark的生成。这样我们就得到了一个带有时间标记的数据流了，后面就能做一些窗口的操作。
      */
    val userBehaviorWithTimeDataStream: DataStream[UserBehavior] = userBehaviorDataStream.assignAscendingTimestamps(_.timestamp * 1000)

    /** 数据格式
      * UserBehavior(844668,1821690,1595193,pv,1511690400)
      * UserBehavior(238114,4782588,1299190,pv,1511690400)
      * UserBehavior(790854,953909,3002561,pv,1511690400)
      * UserBehavior(654062,2899195,3720767,pv,1511690400)
      * UserBehavior(687507,703558,1168596,pv,1511690400)
      */
    // 3.进行业务逻辑的处理：先按照商品id进行分流，再按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window），按每个窗口聚合，输出每个窗口中点击量前N名的商品

    val keyByItemIdStream: KeyedStream[UserBehavior, Long] = userBehaviorWithTimeDataStream.keyBy(_.itemId) // 按照itemId 分流
    val windowStream: WindowedStream[UserBehavior, Long, TimeWindow] = keyByItemIdStream.timeWindow(Time.minutes(60), Time.minutes(5)) // 窗口计算
    windowStream.aggregate() // 做窗口聚合


    // 4.数据的打印


    // 5.任务的执行
    env.execute("hot items job")
  }

}
