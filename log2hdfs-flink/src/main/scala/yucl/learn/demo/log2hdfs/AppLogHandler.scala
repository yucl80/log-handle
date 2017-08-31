package yucl.learn.demo.log2hdfs

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.Properties
import java.util.regex.Pattern

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

object AppLogHandler {
  val logger: Logger = LoggerFactory.getLogger(AppLogHandler.getClass)

  def main(args: Array[String]) {
    val List(bootstrap, topic, consumerGroup, outputPath) = args.toList
    val properties = new Properties
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", consumerGroup)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaConsumer)
    val pattern: Pattern = Pattern.compile("^\\[\\d{2}/\\d{2} ")
    val fullDatePattern: Pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})")
    val logStream = stream.filter(_.nonEmpty)
    logStream.addSink(new SinkFunction[String] {
      override def invoke(msg: String): Unit = {
        var filePath=""
        try {
          val json: Map[String, Any] = JSON.parseFull(msg).get.asInstanceOf[Map[String, Any]]
          val rawMsg = json.getOrElse("message", "").asInstanceOf[String]
          val timestamp = json.getOrElse("@timestamp", "").asInstanceOf[String]
          if (!timestamp.isEmpty) {
            val instant = Instant.parse(timestamp)
            val localTime = instant.atOffset(ZoneOffset.UTC)
            val date = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
            val rawPath = json.getOrElse("path", "").asInstanceOf[String]
            var fileName = rawPath.substring(rawPath.lastIndexOf('/') + 1)
            if (!fullDatePattern.matcher(fileName).find) fileName = fileName + "." + date
            filePath = new StringBuilder().append(outputPath).append("/")
              .append("year=").append(localTime.getYear).append("/")
              .append("month=").append(localTime.getMonthValue).append("/")
              .append("stack=").append(json.getOrElse("stack", "").asInstanceOf[String]).append("/")
              .append("service=").append(json.getOrElse("service", classOf[String])).append("/")
              .append(json.getOrElse("service", "").asInstanceOf[String]).append("-").append(json.getOrElse("index", "").asInstanceOf[String])
              .append(".").append(fileName).toString
            CachedDataFileWriter.write(rawMsg, filePath)
          }else {
            logger.warn("@timestamp not found :"+rawMsg)
          }
        } catch {
          case t: Throwable => logger.error(filePath + ": "+msg, t)
        }
      }
    })
    try {
      env.execute("applog2hdfs")
    } catch {
      case e: Exception => logger.error(e.getMessage, e)

    }
  }
}