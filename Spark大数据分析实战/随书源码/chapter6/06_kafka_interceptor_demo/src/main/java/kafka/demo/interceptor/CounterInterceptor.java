package kafka.demo.interceptor;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 消息发送状态统计拦截器 
 * 统计发送成功和失败的消息数，并在生产者关闭时打印这两个消息数
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {
 private int successCounter = 0;// 发送成功的消息数量
 private int errorCounter = 0;// 发送失败的消息数量
 /**
  * 获取生产者配置信息
  */
 public void configure(Map<String, ?> configs) {
  System.out.println(configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
 }
 /**
  * 该方法在消息发送前调用。
  * 修改发送的消息记录，此处不做处理。
  */
 public ProducerRecord<String, String> onSend(
   ProducerRecord<String, String> record) {
  System.out.println("CounterInterceptor------onSend方法被调用");
  return record;
 }
 /**
  * 该方法在消息发送完毕后调用。
  * 当发送到服务器的记录已被确认，或者记录发送失败时，将调用此方法。
  */
 public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
  System.out.println("CounterInterceptor------onAcknowledgement方法被调用");
  // 统计成功和失败的次数
  if (exception == null) {
   successCounter++;
  } else {
   errorCounter++;
  }
 }
 /**
  * 当生产者关闭时调用该方法，可以在此将结果进行持久化保存
  */
 public void close() {
  System.out.println("CounterInterceptor------close方法被调用");
  // 打印统计结果
  System.out.println("发送成功的消息数量：" + successCounter);
  System.out.println("发送失败的消息数量：" + errorCounter);
 }
}
