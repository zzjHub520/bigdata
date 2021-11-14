package spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 用户自定义函数，隐藏手机号中间四位
  */
object SparkSQLUDF {
   def main(args: Array[String]): Unit = {
      //创建或得到SparkSession
      val spark = SparkSession.builder()
        .appName("SparkSQLUDF")
        .master("local[*]")
        .getOrCreate()

      //第一步：创建测试数据（或直接从文件中读取）
      //模拟数据
      val arr=Array("18001292080","13578698076","13890890876")
      //将数组数据转为RDD
      val rdd: RDD[String] = spark.sparkContext.parallelize(arr)
      //将RDD[String]转为RDD[Row]
      val rowRDD: RDD[Row] = rdd.map(line=>Row(line))
      //定义数据的schema
      val schema=StructType(
         List{
            StructField("phone",StringType,true)
         }
      )
      //将RDD[Row]转为DataFrame
      val df = spark.createDataFrame(rowRDD, schema)

      //第二步：创建自定义函数（phoneHide）
      val phoneUDF=(phone:String)=>{
         var result = "手机号码错误！"
         if (phone != null && (phone.length==11)) {
            val sb = new StringBuffer
            sb.append(phone.substring(0, 3))
            sb.append("****")
            sb.append(phone.substring(7))
            result = sb.toString
         }
         result
      }
      //注册函数（第一个参数为函数名称，第二个参数为自定义的函数）
      spark.udf.register("phoneHide",phoneUDF)

      //第三步：调用自定义函数
      df.createTempView("t_phone")//创建临时视图
      spark.sql("select phoneHide(phone) as phone from t_phone").show()
      // +-----------+
      // |      phone|
      // +-----------+
      // |180****2080|
      // |135****8076|
      // |138****0876|
      // +-----------+
   }

}
