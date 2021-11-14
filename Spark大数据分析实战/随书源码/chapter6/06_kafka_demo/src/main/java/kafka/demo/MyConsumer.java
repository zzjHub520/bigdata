package kafka.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 消费者类
 */
public class MyConsumer {

 public static void main(String[] args) {
  //1. 使用Properties定义配置属性
  Properties props = new Properties();
  //设置消费者 Broker服务器的连接地址
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "centos01:9092,centos02:9092,centos03:9092");
  //设置反序列化key的程序类，与生产者对应
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    StringDeserializer.class.getName());
  //设置反序列化value的程序类，与生产者对应
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    IntegerDeserializer.class.getName());
  //设置消费者组ID，即组名称，值可自定义。组名称相同的消费者进程属于同一个消费者组。
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupid-1");
  //2. 定义消费者对象
  Consumer<String, Integer> consumer = new KafkaConsumer<String, Integer>(props);
  //3. 设置消费者读取的主题名称，可以设置多个
  consumer.subscribe(Arrays.asList("topictest"));
  //4. 不停的读取消息
  while (true) {
   //拉取消息，并设置超时时间为10秒
   ConsumerRecords<String, Integer> records = consumer.poll(Duration
     .ofSeconds(10));
   for (ConsumerRecord<String, Integer> record : records) {
    //打印消息关键信息
    System.out.println("key：" + record.key() + "，value：" + record.value()
      +"，partition："+record.partition()+",offset："+record.offset());
   }
  }
 }
}
