package com.yhs.windowandtime.demo

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val userData: DataStream[UserBehavior] = env.fromCollection(
      List(
        UserBehavior(598575, 766244, 4482016, "pv", 1511690398),
        UserBehavior(834232, 4380938, 4690421, "pv", 1511690398),
        UserBehavior(852512, 848109, 4643350, "pv", 1511690398),
        UserBehavior(528386, 2525052, 2452793, "pv", 1511690398),
        UserBehavior(787890, 1993673, 2732466, "pv", 1511690399),
        UserBehavior(486462, 4309120, 3738615, "pv", 1511690399),
        UserBehavior(286078, 2971425, 903809, "pv", 1511690399)
      )
    )
    userData.print()
    val timeData: DataStream[UserBehavior] = userData.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1000)) {
        /*
            得到最大的有序的时间
         */
        override def getMaxOutOfOrdernessInMillis: Long = {
          println("最大的时间：" + super.getMaxOutOfOrdernessInMillis)
          super.getMaxOutOfOrdernessInMillis

        }

        /*
            提取时间戳
         */
        override def extractTimestamp(element: UserBehavior): Long = {
          element.timestamp
        }

      }
    )
    timeData.printToErr()


    env.execute()


  }

}

/**
  * 用户行为样例类
  *
  * @param userId     用户的id
  * @param itemId     商品id
  * @param categoryId 品类id
  * @param behavior   用户行为 pv cart buy fav 4种
  * @param timestamp  数据产生的时间
  */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)