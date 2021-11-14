package spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 二次排序自定义key类
  * @param first 每一行的第一个字段
  * @param second 每一行的第二个字段
  */
class SecondSortKey(val first:Int,val second:Int) 
  extends Ordered[SecondSortKey] with Serializable {
  /**
    * 实现compare()方法
    */
  override def compare(that: SecondSortKey): Int = {
    //若第一个字段不相等，按照第一个字段升序排列
    if(this.first-that.first!=0){
      this.first-that.first
    }else{//否则按照第二个字段降序排列
      that.second-this.second
    }
  }
}

/**
  * 二次排序运行主类
  */
object SecondSort{
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf()
    //设置应用程序名称，可以在Spark WebUI中显示
    conf.setAppName("Spark-WordCount")
    //设置集群Master节点访问地址，此处为本地模式
    conf.setMaster("local")
    //创建SparkContext对象,该对象是提交Spark应用程序的入口
    val sc = new SparkContext(conf);

    //1. 读取指定路径的文件内容，生成一个RDD集合
    val lines:RDD[String] = sc.textFile("D:\\test\\sort.txt")
    //2. 将RDD中的元素转为(SecondSortKey, String)形式的元组
    val pair: RDD[(SecondSortKey, String)] = lines.map(line => (
      new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
      line)
    )
    //3. 按照元组的key（SecondSortKey的实例）进行排序
    var pairSort: RDD[(SecondSortKey, String)] = pair.sortByKey()
    //取排序后的元组中的第二个值（value值）
    val result: RDD[String] = pairSort.map(line=>line._2)
    //打印最终结果
    result.foreach(line=>println(line))
  }
}
