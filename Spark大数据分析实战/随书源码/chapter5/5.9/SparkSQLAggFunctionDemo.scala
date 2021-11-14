package spark.demo

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 统计用户UV（每天的用户访问量）
  */
object SparkSQLAggFunctionDemo {
   def main(args: Array[String]): Unit = {
      //创建或得到SparkSession
      val spark = SparkSession.builder()
        .appName("SparkSQLAggFunctionDemo")
        .master("local[*]")
        .getOrCreate()

      //导入函数
      import org.apache.spark.sql.functions._
      //构造测试数据（第一列：日期，第二列：用户ID）
      val arr=Array(
         "2019-06-01,0001",
         "2019-06-01,0001",
         "2019-06-01,0002",
         "2019-06-01,0003",
         "2019-06-02,0001",
         "2019-06-02,0003"
      )
      //转为RDD[Row]
      val rowRDD=spark.sparkContext.makeRDD(arr).map(line=>
           Row(line.split(",")(0),line.split(",")(1).toInt)
        )

      //构建DataFrame元数据
      val structType=StructType(Array(
         StructField("date",StringType,true),
         StructField("userid",IntegerType,true)
      ))

      //将RDD[Row]转为DataFrame
      val df=spark.createDataFrame(rowRDD,structType)
      //聚合查询
      //根据日期分组，然后将每一组的用户ID去重后统计数量
      df.groupBy("date")
        .agg(countDistinct("userid") as "count")
        .show()
        // +----------+-----+
        // |       date|count|
        // +----------+-----+
        // |2019-06-02|    2|
        // |2019-06-01|    3|
        // +----------+-----+
   }
}
