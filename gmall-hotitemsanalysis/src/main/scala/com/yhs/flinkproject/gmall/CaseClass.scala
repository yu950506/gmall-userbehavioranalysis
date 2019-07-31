package com.yhs.flinkproject.gmall

/*
    这里存放所有的样例类，方便我们封装数据，操作对象
 */
object CaseClass {
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


/**
  * 热门商品样例类
  *
  * @param itemId    商品id
  * @param windowEnd 窗口的结束时间
  * @param count     点击的总次数
  */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)