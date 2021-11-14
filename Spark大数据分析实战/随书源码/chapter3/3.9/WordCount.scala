package spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark RDD单词计数程序
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf()
    //设置应用程序名称，可以在Spark WebUI中显示
    conf.setAppName("Spark-WordCount")
    //设置集群Master节点访问地址
    conf.setMaster("spark://centos01:7077");

    //创建SparkContext对象,该对象是提交Spark应用程序的入口
    val sc = new SparkContext(conf);

    //读取指定路径(取程序执行时传入的第一个参数)中的文件内容，生成一个RDD集合
    val linesRDD:RDD[String] = sc.textFile(args(0))
    //将RDD数据按照空格进行切分并合并为一个新的RDD
    val wordsRDD:RDD[String] = linesRDD.flatMap(_.split(" "))
    //将RDD中的每个单词和数字1放到一个元组里，即(word,1)
    val paresRDD:RDD[(String, Int)] = wordsRDD.map((_,1))
    //对单词根据key进行聚合，对相同的key进行value的累加
    val wordCountsRDD:RDD[(String, Int)] = paresRDD.reduceByKey(_+_)
    //按照单词数量降序排列
    val wordCountsSortRDD:RDD[(String, Int)] = wordCountsRDD.sortBy(_._2,false)
    //保存结果到指定的路径(取程序执行时传入的第二个参数)
    wordCountsSortRDD.saveAsTextFile(args(1))
    //停止SparkContext，结束该任务
    sc.stop();
  }
}


