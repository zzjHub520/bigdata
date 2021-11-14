package spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark RDD统计每日新增用户
  */
object DayNewUser {
   def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("DayNewUser")
      conf.setMaster("local[*]")

      val sc = new SparkContext(conf)
      //1. 构建测试数据
      val tupleRDD:RDD[(String,String)] = sc.parallelize(
         Array(
            ("2020-01-01", "user1"),
            ("2020-01-01", "user2"),
            ("2020-01-01", "user3"),
            ("2020-01-02", "user1"),
            ("2020-01-02", "user2"),
            ("2020-01-02", "user4"),
            ("2020-01-03", "user2"),
            ("2020-01-03", "user5"),
            ("2020-01-03", "user6")
         )
      )
      //2. 倒排（互换RDD中元组的元素顺序）
      val tupleRDD2:RDD[(String,String)] = tupleRDD.map(
      line => (line._2, line._1)
      )
      //3. 将倒排后的RDD按照key分组
      val groupedRDD: RDD[(String, Iterable[String])] = tupleRDD2.groupByKey()
      //4. 取分组后的每个日期集合中的最小日期，并计数为1
      val dateRDD:RDD[(String,Int)] = groupedRDD.map(
         line => (line._2.min, 1)
      )
      //5. 计算所有相同key（即日期）的数量
      val resultMap: collection.Map[String, Long] = dateRDD.countByKey()
      //将结果Map循环打印到控制台
      resultMap.foreach(println)

   }

}
