package spark.demo

import scala.collection.mutable.Queue
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * RDD队列流例子
  */
object QueueStream {

  def main(args: Array[String]) {
    //创建sparkConf对象
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("QueueStream")
    //创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //创建队列，用于存放RDD
    val rddQueue = new Queue[RDD[Int]]()
    //创建输入DStream（以队列为参数）
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //输出计算结果
    reducedStream.print()
    ssc.start()

    //每隔1秒创建一个RDD并将其推入队列rddQueue中（循环30次，则共创建了30个RDD）
    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(1000)
    }
    ssc.stop()
  }
}
