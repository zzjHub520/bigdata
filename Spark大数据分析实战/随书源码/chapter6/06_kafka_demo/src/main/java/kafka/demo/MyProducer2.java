package kafka.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**生产者类（添加了注释部分）**/
public class MyProducer2 {
	
	public static void main(String[] args) {
		//1. 使用properties定义kafka环境属性
		Properties props = new Properties();
		//设置生产者 broker服务器连接地址
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos01:9092,centos02:9092,centos03:9092");
		//设置序列化key程序类
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//设置序列化value程序类，此处不一定非得是Integer，也可以是String
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		//2. 定义消息生产者对象，依靠此对象可以进行消息的传递
		Producer<String,Integer> producer=new KafkaProducer<String,Integer>(props);
		
		//3. 循环发送10条消息
		for(int i=0;i<10;i++){
			//发送消息，第一个参数为主题名称，第二个参数为消息的key值，第三个参数为消息的value值
			//此方式只负责发送消息，不知道是否发送成功
			producer.send(new ProducerRecord<String,Integer>("topictest","hello kafka "+i,i));
			
			/*//同步方式发送消息
			//使用生产者对象的send()方法发送消息，会返回一个Future对象，然后调用Future对象的get()方法进行等待，就可以知道消息是否发送成功。如果服务器返回错误，get()方法会抛出异常。如果没有发生错误，我们会得到一个RecordMetadata对象，可以用它获取消息的偏移量。最简单的同步发送消息的代码如下：
			try {
				producer.send(new ProducerRecord<String,Integer>("topictest","hello kafka "+i,i)).get();
			} catch (Exception e) {
				e.printStackTrace();
			}*/
			
			/*//异步方式发送消息，可以指定一个回调函数，服务器返回响应时会调用该函数。我们可以在该函数中对一些异常信息进行处理，比如记录错误日志或者把消息写入“错误消息”文件以便日后分析。
			producer.send(new ProducerRecord<String,Integer>("topictest","hello kafka "+i,i),new Callback(){
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if(e!=null){
						e.printStackTrace();
					}
				}
			});*/
			
		}
		//4.关闭生产者
		producer.close();
	}
	
}
