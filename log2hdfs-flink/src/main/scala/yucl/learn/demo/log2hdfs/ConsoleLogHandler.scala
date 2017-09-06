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

object ConsoleLogHandler {
  val logger: Logger = LoggerFactory.getLogger(ConsoleLogHandler.getClass)

  def main(args: Array[String]) {
    val List(bootstrap, topic, consumerGroup, outputPath) = args.toList

    val properties = new Properties
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", consumerGroup)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000)
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaConsumer).name(bootstrap + "/" + topic + ":" + consumerGroup)
    val dockerLogTimePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})")
    val logStream = stream.filter(_.nonEmpty)
      .map(msg => {
        var result: Option[(String, String)] = None
        try {
          val parseResult = JSON.parseFull(msg)
          if (parseResult != None) {
            val json = parseResult.get.asInstanceOf[Map[String, Any]]
            val timestamp = json.get("@timestamp")
            val rawMsg = json.getOrElse("message","").asInstanceOf[String]
            if (timestamp != None) {
              val instant = Instant.parse(timestamp.get.asInstanceOf[String])
              val localTime = instant.atZone(ZoneId.of("Asia/Shanghai"))
              val dateStr = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
              if (!json.getOrElse("stack", "").asInstanceOf[String].isEmpty) {
                val filePath = new StringBuilder().append(outputPath).append("/")
                  .append("year=").append(dateStr.substring(0, 4)).append("/")
                  .append("month=").append(dateStr.substring(5, 7).toInt).append("/")
                  .append("stack=").append(json.getOrElse("stack", "").asInstanceOf[String]).append("/")
                  .append("service=").append(json.getOrElse("service", "").asInstanceOf[String]).append("/")
                  .append(json.getOrElse("service", "").asInstanceOf[String]).append("-").append(json.getOrElse("index", "").asInstanceOf[String])
                  .append(".").append("console.out").append(".").append(dateStr).toString
                result = Some((rawMsg, filePath))
              }
            } else {
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
      env.execute("containerlog2hdfs")
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }

  }



}
