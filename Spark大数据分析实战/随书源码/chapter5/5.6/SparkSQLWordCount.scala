package spark.demo

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
/**
  * Spark SQL单词计数程序
  */
object SparkSQLWordCount {

  def main(args: Array[String]): Unit = {
    //创建SparkSession对象,并设置应用名称、运行模式
    val session=SparkSession.builder()
      .appName("SparkSQLWordCount")
      .master("local[*]")
      .getOrCreate()
    
    //读取HDFS中的单词文件
    val lines: Dataset[String] = session.read.textFile(
      "hdfs://centos01:9000/input/words.txt")
    lines.show()
    // +-------------+
    // |        value |
    // +-------------+
    // |hello hadoop|
    // |   hello java|
    // |  hello scala|
    // |        java   |
    // +-------------+
    //导入session对象中的隐式转换
    import session.implicits._
    //将Dataset中的数据按照空格进行切分并合并
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    words.show()
    // +------+
    // | value|
    // +------+
    // | hello|
    // |hadoop|
    // | hello|
    // |  java|
    // | hello|
    // | scala|
    // |  java|
    // +------+
    //将Dataset中默认的列名value改为word，同时把Dataset转为DataFrame
    val df: DataFrame = words.withColumnRenamed("value","word")
    df.show()
    // +------+
    // |  word|
    // +------+
    // | hello|
    // |hadoop|
    // | hello|
    // |  java|
    // | hello|
    // | scala|
    // |  java|
    // +------+
    //给DataFrame创建临时视图
    df.createTempView("v_words")
    //执行SQL，从DataFrame中查询数据，按照单词进行分组
    val result: DataFrame = session.sql(
      "select word,count(*) as count from v_words group by word order by count desc")
    //显示查询结果
    result.show()
    // +------+-----+
    // |  word|count|
    // +------+-----+
    // | hello|    3|
    // |  java|    2|
    // |hadoop|    1|
    // | scala|    1|
    // +------+-----+
    //关闭SparkSession
    session.close()
  }
}
