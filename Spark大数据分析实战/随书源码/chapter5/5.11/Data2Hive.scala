package spark.demo

import org.apache.spark.sql.SparkSession
/**
  * 数据导入Hive
  */
object Data2Hive {
   def main(args: Array[String]): Unit = {
      //构建SparkSession
      val spark=SparkSession.builder()
        .appName("Data2Hive")
        .enableHiveSupport()//开启Hive支持
        .getOrCreate()

      //使用数据库traffic_db
      spark.sql("USE traffic_db");
      spark.sql("DROP TABLE IF EXISTS monitor_flow_action");
      //在hive中创建monitor_flow_action表
      spark.sql("CREATE TABLE IF NOT EXISTS monitor_flow_action " +
        "(date STRING,monitor_id STRING,camera_id STRING,car STRING," +
        "action_time STRING,speed STRING,road_id STRING,area_id STRING) " +
        "row format delimited fields terminated by '\t' ")
      //导入数据到表monitor_flow_action
      spark.sql("load data local inpath " +
        "'/opt/softwares/resources/monitor_flow_action' " +
        "into table monitor_flow_action")

      //在hive中创建monitor_camera_info表
      spark.sql("DROP TABLE IF EXISTS monitor_camera_info")
      spark.sql("CREATE TABLE IF NOT EXISTS monitor_camera_info " +
        "(monitor_id STRING, camera_id STRING) " +
        "row format delimited fields terminated by '\t'")
      //导入数据到表monitor_camera_info
      spark.sql("LOAD DATA LOCAL INPATH " +
        "'/opt/softwares/resources/monitor_camera_info' " +
        "INTO TABLE monitor_camera_info")

      System.out.println("========data2hive finish========")
   }

}
