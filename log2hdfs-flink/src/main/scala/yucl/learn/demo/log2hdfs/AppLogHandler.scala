package yucl.learn.demo.log2hdfs

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.Properties
import java.util.regex.Pattern

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
    val stream = env.addSource(kafkaConsumer).name(bootstrap + "/" + topic + ":" + consumerGroup)
    val fullDatePattern: Pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})")
    val logStream = stream
      .filter(_.nonEmpty)
      .map(msg => {
        var result: Option[(String, String)] = None
        try {
          val parseResult = JSON.parseFull(msg)
          if (parseResult != None) {
            val json = parseResult.get.asInstanceOf[Map[String, Any]]
            val rawMsg = json.getOrElse("message", "").asInstanceOf[String]
            val timestamp = json.getOrElse("@timestamp", "").asInstanceOf[String]
            if (!timestamp.isEmpty) {
              val instant = Instant.parse(timestamp)
              val localTime = instant.atZone(ZoneId.of("Asia/Shanghai"))
              val date = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
              val rawPath = json.getOrElse("path", "").asInstanceOf[String]
              var fileName = rawPath.substring(rawPath.lastIndexOf('/') + 1)
              if (!fullDatePattern.matcher(fileName).find) fileName = fileName + "." + date
              val filePath = new StringBuilder().append(outputPath).append("/")
                .append("year=").append(localTime.getYear).append("/")
                .append("month=").append(localTime.getMonthValue).append("/")
                .append("stack=").append(json.getOrElse("stack", "").asInstanceOf[String]).append("/")
                .append("service=").append(json.getOrElse("service", classOf[String])).append("/")
                .append(json.getOrElse("service", "").asInstanceOf[String]).append("-").append(json.getOrElse("index", "").asInstanceOf[String])
                .append(".").append(fileName).toString
              result = Some((rawMsg, filePath))
            }else {
              logger.warn("@timestamp not found:" + msg)
            }
          } else {
            logger.warn("parse msg failed:" + msg)
          }
        } catch {
          case t: Throwable => logger.error("hand message failed :" + msg, t)
        }
        result
      })
      .filter(_ != None)
      .map(_.get)

    logStream.addSink(new CachedHdfsWriterSink()).name("write:" + outputPath)

    try {
      env.execute(topic+"2hdfs")
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }
}