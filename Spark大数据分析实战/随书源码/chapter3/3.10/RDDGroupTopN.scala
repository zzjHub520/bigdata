package spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
  * Spark分组取TopN程序
  */
object RDDGroupTopN {
   def main(args: Array[String]): Unit = {
      //创建SparkConf对象，存储应用程序的配置信息
      val conf = new SparkConf()
      //设置应用程序名称，可以在Spark WebUI中显示
      conf.setAppName("RDDGroupTopN")
      //设置集群Master节点访问地址，此处为本地模式
      conf.setMaster("local[*]")

      val sc = new SparkContext(conf)
      //1. 加载本地数据
      val linesRDD: RDD[String] = sc.textFile("D:/input/score.txt")

      //2. 将RDD元素转为(String,Int)形式的元组
      val tupleRDD:RDD[(String,Int)]=linesRDD.map(line=>{
         val name=line.split(",")(0)
         val score=line.split(",")(1)
         (name,score.toInt)
      })

      //3. 按照key（姓名）进行分组
      val top5=tupleRDD.groupByKey().map(groupedData=>{
         val name:String=groupedData._1
         //每一组的成绩降序后取前3个
         val scoreTop3:List[Int]=groupedData._2
           .toList.sortWith(_>_).take(3)
         (name,scoreTop3)//返回元组
      })

      //4. 循环打印分组结果
      top5.foreach(tuple=>{
         println("姓名："+tuple._1)
         val tupleValue=tuple._2.iterator
         while (tupleValue.hasNext){
            val value=tupleValue.next()
            println("成绩："+value)
         }
         println("*******************")
      })
   }
}
