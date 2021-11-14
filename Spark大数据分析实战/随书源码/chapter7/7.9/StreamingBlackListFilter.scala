package spark.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming实时用户日志黑名单过滤
  */
object StreamingBlackListFilter {

   //所有org包名只输出ERROR级别的日志，如果导入其他包只需要再新创建一行写入包名即可
   Logger.getLogger("org").setLevel(Level.ERROR)

   def main(args: Array[String]) {

      //创建SparkConf对象
      val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("StreamingBlackListFilter")
      //创建Spark Streaming上下文对象，并以1秒内收到的数据作为一个批次
      val ssc = new StreamingContext(conf, Seconds(1))

      //1. 构建黑名单数据
      val blackList=Array(("tom",true),("leo",true));
      //将黑名单数据转为RDD
      val blackListRDD=ssc.sparkContext.makeRDD(blackList)

      //2. 创建输入DStream，从Socket中接收用户访问数据，格式：20191012 tom
      val logDStream = ssc.socketTextStream("centos01", 9999)
      //将输入DStream转换为元组，便于后面的连接查询
      val logTupleDStream=logDStream.map(line=>{
         (line.split(" ")(1),line)//(tom,20191012 tom)
      })

      //3. 将输入DStream进行转换操作，左外连接黑名单数据并过滤掉黑名单用户数据
      val resultDStream=logTupleDStream.transform(rdd=>{
         //将用户访问数据与黑名单数据进行左外连接
         val leftJoinRDD=rdd.leftOuterJoin(blackListRDD)
         //过滤算子filter(func)，保留函数func运算结果为true的元素
         val filteredRDD=leftJoinRDD.filter(line=>{
            print(line+"------"+line._2._2.getOrElse(false))
            //(leo,(20191012 leo,Some(true)))------false
            //(jack,(20191012 jack,None))------true

            //能取到值说明有相同的key，因此返回false将其过滤掉；
            //取不到值返回true，保留该值
            if(line._2._2.getOrElse(false)){
               false
            }else{
               true
            }
         })
         //取出原始用户访问数据
         val resultRDD=filteredRDD.map(line=>line._2._1)
         resultRDD
      })

      //4. 打印过滤掉黑名单用户后的日志数据
      resultDStream.print()
      //20191012 jack

      ssc.start() //启动计算
      ssc.awaitTermination() //等待计算结束
   }

}
