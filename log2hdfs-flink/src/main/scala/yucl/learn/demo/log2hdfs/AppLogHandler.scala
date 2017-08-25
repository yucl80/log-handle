package yucl.learn.demo.log2hdfs

import java.util.regex.Pattern
import java.util.{Calendar, Properties}

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

object AppLogHandler {
  val logger: Logger = LoggerFactory.getLogger(AppLogHandler.getClass)

  def main(args: Array[String]) {
    val List(bootstrap, topic, consumerGroup, outputPath) = args.toList
    //val List(bootstrap, topic, consumerGroup, outputPath) =
     //List("10.62.14.66:9092","applog","test","hdfs://10.62.14.46:9000/tmp/applog1")
    val properties = new Properties
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", consumerGroup)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaConsumer)
    val pattern: Pattern = Pattern.compile("^\\[\\d{2}/\\d{2} ")
    val fullDatePattern: Pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}) ")
    val logStream = stream.filter(_.nonEmpty)
    logStream.addSink(new SinkFunction[String] {
      override def invoke(msg: String): Unit = {
        try {
          val json: Map[String, Any] = JSON.parseFull(msg).get.asInstanceOf[Map[String, Any]]
          val rawMsg = json.getOrElse("message", "").asInstanceOf[String]
          var date = ""
          val matcher = fullDatePattern.matcher(rawMsg)
          if (matcher.find) date = matcher.group(1)
          else if (pattern.matcher(rawMsg).find) {
            val year = String.valueOf(Calendar.getInstance.get(Calendar.YEAR))
            val eventTime = year + "-" + rawMsg.substring(1, 6)
            date = eventTime.replaceAll("/", "-")
          }
          val rawPath = json.getOrElse("path", "").asInstanceOf[String]
          var fileName = rawPath.substring(rawPath.lastIndexOf('/') + 1)
          if (!fullDatePattern.matcher(fileName).find) fileName = fileName + "." + date
          val filePath = new StringBuilder().append(outputPath).append("/")
            .append("year=").append(date.substring(0, 4)).append("/")
            .append("month=").append(date.substring(5, 7).toInt).append("/")
            .append("stack=").append(json.getOrElse("stack", "").asInstanceOf[String]).append("/")
            .append("service=").append(json.getOrElse("service", classOf[String])).append("/")
            .append(json.getOrElse("service", "").asInstanceOf[String]).append("-").append(json.getOrElse("index", "").asInstanceOf[String])
            .append(".").append(fileName).toString
          CachedDataFileWriter.write(rawMsg, filePath)
        } catch {
          case t: Throwable => logger.error(msg, t)
        }
      }
    })
    try {
      env.execute("applog2hdfs")
    } catch {
      case e: Exception =>logger.error(e.getMessage, e)

    }

  }

}
