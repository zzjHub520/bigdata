package spark.demo

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * 自定义Receiver类
  * @param host 数据源的域名/IP
  * @param port 数据源的端口
  */
class MyReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  /**
    * Receiver启动时调用
    */
  def onStart() {
    // 启动通过Socket连接接收数据的线程
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  /**
    * Receiver停止时调用
    */
  def onStop() {
    //如果isStopped()返回false，调用receive()方法的线程将自动停止，
    //因此此处无需做太多工作
  }

  /**
    * 创建Socket连接并接收数据，直到Receiver停止
    */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // 连接到host:port
      socket = new Socket(host, port)

      // 读取数据，直到Receiver停止或连接中断
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)//存储数据到内存
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // 重新启动，以便在服务器再次激活时重新连接
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // 如果无法连接到服务器，则重新启动
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // 如果有任何其他错误，则重新启动
        restart("Error receiving data", t)
    }
  }
}
