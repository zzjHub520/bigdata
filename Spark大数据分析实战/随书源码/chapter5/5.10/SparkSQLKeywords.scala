package spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
/**
  * 每天热点搜索关键词统计
  */
object SparkSQLKeywords {

  def main(args: Array[String]): Unit = {

    //构建SparkSession
    val spark=SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    /**1.加载数据，转换数据**********************/
    //读取HDFS数据，创建RDD
    val linesRDD: RDD[String] = spark.sparkContext.textFile("D:/test/keywords.txt")
    //将RDD元素转为((日期,关键词),用户)格式的元组
    val tupleRDD: RDD[((String, String), String)] = linesRDD.map(line => {
      val date = line.split(",")(0)//日期
      val user = line.split(",")(1)//用户
      val keyword = line.split(",")(2)//关键词
      ((date, keyword), user)
    })
    //根据(日期,关键词)进行分组，获取每天每个搜索词被哪些用户进行了搜索
    val groupedRDD: RDD[((String, String), Iterable[String])] = tupleRDD.groupByKey()
    //对每天每个搜索词的用户进行去重，并统计去重后的数量,获得其uv
    val uvRDD: RDD[((String, String), Int)] = groupedRDD.map(line => {
      val dateAndKeyword: (String, String) = line._1
      //用户数据去重
      val users: Iterator[String] = line._2.iterator
      val distinctUsers = new ListBuffer[String]()
      while (users.hasNext) {
        val user = users.next
        if (!distinctUsers.contains(user)) {
          distinctUsers += user
        }
      }
      val uv = distinctUsers.size //数量即uv
      //返回((日期,关键词),uv)
      (dateAndKeyword, uv)
    })

    /**2.转为DataFrame************************/
    //转为RDD[Row]
    val rowRDD: RDD[Row] = uvRDD.map(line => {
      Row(
        line._1._1, //日期
        line._1._2, //关键词
        line._2.toInt //uv
      )
    })
    //构建DataFrame元数据
    val structType=StructType(Array(
      StructField("date",StringType,true),
      StructField("keyword",StringType,true),
      StructField("uv",IntegerType,true)
    ))
    //将RDD[Row]转为DataFrame
    val df=spark.createDataFrame(rowRDD,structType)
    df.createTempView("date_keyword_uv")

    /**3.执行SQL查询************************/
    // 使用Spark SQL的开窗函数，统计每天搜索uv排名前3的搜索词
    spark.sql(""
      + "SELECT date,keyword,uv "
      + "FROM ("
        + "SELECT "
        + "date,"
        + "keyword,"
        + "uv,"
        + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
        + "FROM date_keyword_uv "
      + ") t "
      + "WHERE t.rank<=3").show()
    // +----------+----------+---+
    // |       date|    keyword| uv|
    // +----------+----------+---+
    // |2019-10-03|    名胜古迹|  1|
    // |2019-10-03|      小吃街|  1|
    // |2019-10-01|      小吃街|  3|
    // |2019-10-01|        烤肉|  2|
    // |2019-10-01|  谷歌浏览器|  1|
    // |2019-10-02|    名胜古迹|  2|
    // |2019-10-02|    安全卫士|  1|
    // +----------+----------+---+

    //关闭SparkSession
    spark.close()
  }
}
