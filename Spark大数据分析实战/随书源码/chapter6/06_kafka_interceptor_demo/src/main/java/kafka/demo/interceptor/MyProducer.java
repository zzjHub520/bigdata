package kafka.demo.interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 生产者类
 */
public class MyProducer {

 public static void main(String[] args) {
  //1. 设置配置属性
  Properties props = new Properties();
  // 设置生产者Broker服务器连接地址
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "centos01:9092,centos02:9092,centos03:9092");
  // 设置序列化key程序类
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    StringSerializer.class.getName());
  // 设置序列化value程序类，此处不一定非得是Integer，也可以是String
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    StringSerializer.class.getName());

  //2. 设置拦截器链
  List<String> interceptors = new ArrayList<String>();
  // 添加拦截器TimeInterceptor（需指定拦截器的全路径）
  interceptors.add("kafka.demo.interceptor.TimeInterceptor");
  // 添加拦截器CounterInterceptor
  interceptors.add("kafka.demo.interceptor.CounterInterceptor");
  // 将拦截器加入到配置属性中
  props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

  //3. 发送消息
  Producer<String, String> producer = new KafkaProducer<String, String>(props);
  // 循环发送5条消息
  for (int i = 0; i < 5; i++) {
   // 发送消息，此方式只负责发送消息，不关心是否发送成功
   // 第一个参数:主题名称
   // 第二个参数：消息的value值（消息内容）
   producer.send(new ProducerRecord<String, String>("topictest", "hello kafka "
     + i));
   
  }
  // 4.关闭生产者，释放资源
  // 调用该方法后将触发拦截器的close()方法
  producer.close();
 }
}
