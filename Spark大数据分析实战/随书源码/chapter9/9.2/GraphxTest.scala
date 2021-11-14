package spark.demo

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * GraphX计算年龄大于25的顶点（用户）
  */
object GraphxTest {
   def main(args: Array[String]): Unit = {
      //创建SparkConf对象
      val conf = new SparkConf()
      conf.setAppName("Spark-GraphXDemo")
      conf.setMaster("local[2]");
      //创建SparkContext对象
      val sc = new SparkContext(conf);
      //设置日志级别
      sc.setLogLevel("WARN")

      //1. 创建顶点集合和边集合，注意顶点集合和边集合都是元素类型为元组的Array
      //创建顶点集合
      val vertexArray = Array(
      (1L,("Alice", 30)),
      (2L,("Henry", 27)),
      (3L,("Charlie", 25)),
      (4L,("Peter", 22)),
      (5L,("Mike", 29)),
      (6L,("Kate", 23))
      )

      //创建边集合
      val edgeArray = Array(
      Edge(2L, 1L, "关注"),
      Edge(2L, 4L, "喜欢"),
      Edge(3L, 2L, "关注"),
      Edge(3L, 6L, "关注"),
      Edge(5L, 2L, "喜欢"),
      Edge(5L, 3L, "关注"),
      Edge(5L, 6L, "关注")
      )

      //2. 构造顶点RDD和边RDD
      val vertexRDD:RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)
      val edgeRDD:RDD[Edge[String]] = sc.parallelize(edgeArray)

      //3. 构造GraphX图
      val graph:Graph[(String,Int),String] = Graph(vertexRDD, edgeRDD)
      println("图中年龄大于25的顶点:")
      //过滤图中满足条件的顶点
      graph.vertices.filter(v => v._2._2>25).collect.foreach {
         v => println(s"${v._2._1} 年龄是 ${v._2._2}")
      }
   }
}
