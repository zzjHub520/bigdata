package kafka.demo.interceptor;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 时间戳拦截器 发送消息之前，在消息内容前面加入时间戳
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
 /**
  * 获取生产者配置信息
  */
 public void configure(Map<String, ?> configs) {
  System.out.println(configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
 }
 /**
  * 该方法在消息发送前调用。
  * 将原消息记录进行修改，在消息内容最前边添加时间戳。
  * @param record
  *         生产者发送的消息记录，将自动传入
  * @return 修改后的消息记录
  */
 public ProducerRecord<String, String> onSend(
   ProducerRecord<String, String> record) {
  System.out.println("TimeInterceptor------onSend方法被调用");
  // 创建一条新的消息记录，将时间戳加入消息内容的最前边
  ProducerRecord<String, String> proRecord = new ProducerRecord<String, String>(
    record.topic(), record.key(), System.currentTimeMillis() + ","
      + record.value().toString());
  return proRecord;
 }
 /**
  * 该方法在消息发送完毕后调用。
  * 当发送到服务器的记录已被确认，或者记录发送失败时，将调用此方法。
  */
 public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
  System.out.println("TimeInterceptor------onAcknowledgement方法被调用");
 }
 /**
  * 当拦截器关闭时调用该方法
  */
 public void close() {
  System.out.println("TimeInterceptor------close方法被调用");
 }
}
