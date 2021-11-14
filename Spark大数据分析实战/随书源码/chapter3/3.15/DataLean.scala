package spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

/**
  * Spark RDD解决数据倾斜案例
  */
object DataLean {
   def main(args: Array[String]): Unit = {
      //创建Spark配置对象
      val conf = new SparkConf();
      conf.setAppName("DataLean")
      conf.setMaster("spark://centos01:7077");

      //创建SparkContext对象
      val sc = new SparkContext(conf);

      //1. 读取测试数据
      val linesRDD = sc.textFile("hdfs://centos01:9000/input/words.txt");
      //2. 统计单词数量
      linesRDD
        .flatMap(_.split(" "))
        .map((_, 1))
        .map(t => {
           val word = t._1
           val random = Random.nextInt(100)//产生0~99的随机数
           //单词加入随机数前缀，格式：(前缀_单词,数量)
           (random + "_" + word, 1)
        })
        .reduceByKey(_ + _)//局部聚合
        .map(t => {
         val word = t._1
         val count = t._2
         val w = word.split("_")(1)//去除前缀
         //单词去除随机数前缀，格式：(单词,数量)
         (w, count)
      })
        .reduceByKey(_ + _)//全局聚合
        //输出结果到指定的HDFS目录
        .saveAsTextFile("hdfs://centos01:9000/output/")
   }
}
