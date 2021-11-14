package spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求成绩平均分
  */
object AverageScore {
   def main(args: Array[String]): Unit = {
      //创建SparkConf对象，存储应用程序的配置信息
      val conf = new SparkConf()
      //设置应用程序名称，可以在Spark WebUI中显示
      conf.setAppName("AverageScore")
      //设置集群Master节点访问地址
      conf.setMaster("spark://centos01:7077")

      val sc = new SparkContext(conf)
      //1. 加载数据
      val linesRDD: RDD[String] = sc.textFile("hdfs://centos01:9000/input")
      //2. 将RDD中的元素转为(key,value)形式，便于后面进行聚合
      val tupleRDD: RDD[(String, Int)] = linesRDD.map(line => {
         val name = line.split("\t")(0)//姓名
         val score = line.split("\t")(1).toInt//成绩
         (name, score)
      })
      //3. 根据姓名进行分组，形成新的RDD
      val groupedRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
      //4. 迭代计算RDD中每个学生的平均分
      val resultRDD: RDD[(String, Int)] = groupedRDD.map(line => {
         val name = line._1//姓名
         val iteratorScore: Iterator[Int] = line._2.iterator//成绩迭代器
         var sum = 0//总分
         var count = 0//科目数量

         //迭代累加所有科目成绩
         while (iteratorScore.hasNext) {
            val score = iteratorScore.next()
            sum += score
            count += 1
         }
         //计算平均分
         val averageScore = sum / count
         (name, averageScore)//返回（姓名，平均分）形式的元组
      })
      //保存结果
      resultRDD.saveAsTextFile("hdfs://centos01:9000/output")
   }
}

