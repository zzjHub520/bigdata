package spark.demo

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 统计每一个产品类别的销售额前三名（相当于分组求TOPN）
  */
object SparkSQLWindowFunctionDemo {
   def main(args: Array[String]): Unit = {
      //创建或得到SparkSession
      val spark = SparkSession.builder()
        .appName("SparkSQLWindowFunctionDemo")
        .master("local[*]")
        .getOrCreate()

      //第一步：创建测试数据（字段：日期，产品类别，销售额）
      val arr=Array(
         "2019-06-01,A,500",
         "2019-06-01,B,600",
         "2019-06-01,C,550",
         "2019-06-02,A,700",
         "2019-06-02,B,800",
         "2019-06-02,C,880",
         "2019-06-03,A,790",
         "2019-06-03,B,700",
         "2019-06-03,C,980",
         "2019-06-04,A,920",
         "2019-06-04,B,990",
         "2019-06-04,C,680"
      )
      //转为RDD[Row]
      val rowRDD=spark.sparkContext
        .makeRDD(arr)
        .map(line=>Row(
              line.split(",")(0),
              line.split(",")(1),
              line.split(",")(2).toInt
        ))
      //构建DataFrame元数据
      val structType=StructType(Array(
         StructField("date",StringType,true),
         StructField("type",StringType,true),
         StructField("money",IntegerType,true)
      ))
      //将RDD[Row]转为DataFrame
      val df=spark.createDataFrame(rowRDD,structType)

      //第二步：使用开窗函数，取每一个类别的金额前三名
      df.createTempView("t_sales")//创建临时视图
      //执行SQL查询
      spark.sql(
        "select date,type,money,rank from " +
          "(select date,type,money," +
         "row_number() over (partition by type order by money desc) rank "+
          "from t_sales) t " +
        "where t.rank<=3"
      ).show()
      // +----------+----+-----+----+
      // |      date|type|money|rank|
      // +----------+----+-----+----+
      // |2019-06-04|   B|  990|   1|
      // |2019-06-02|   B|  800|   2|
      // |2019-06-03|   B|  700|   3|
      // |2019-06-03|   C|  980|   1|
      // |2019-06-02|   C|  880|   2|
      // |2019-06-04|   C|  680|   3|
      // |2019-06-04|   A|  920|   1|
      // |2019-06-03|   A|  790|   2|
      // |2019-06-02|   A|  700|   3|
      // +----------+----+-----+----+
   }
}
