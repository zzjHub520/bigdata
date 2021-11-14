package kafka.demo;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 生产者类
 */
public class MyProducer {

 public static void main(String[] args) {
  //1. 使用Properties定义配置属性
  Properties props = new Properties();
  //设置生产者Broker服务器连接地址
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "centos01:9092,centos02:9092,centos03:9092");
  //设置序列化key程序类
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    StringSerializer.class.getName());
  //设置序列化value程序类，此处不一定非得是Integer，也可以是String
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    IntegerSerializer.class.getName());
  //2. 定义消息生产者对象，依靠此对象可以进行消息的传递
  Producer<String, Integer> producer = new KafkaProducer<String, Integer>(props);

  //3. 循环发送10条消息
  for (int i = 0; i < 10; i++) {
   //发送消息，此方式只负责发送消息，不关心是否发送成功
   //第一个参数:主题名称
   //第二个参数：消息的key值
   //第三个参数：消息的value值
   producer.send(new ProducerRecord<String, Integer>("topictest",
     "hello kafka " + i, i));
  }
  //4.关闭生产者，释放资源
  producer.close();
 }
}
