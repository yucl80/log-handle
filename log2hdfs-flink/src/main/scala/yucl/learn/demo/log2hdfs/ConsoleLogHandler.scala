package yucl.learn.demo.log2hdfs

import java.text.{ParseException, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Date, Properties}

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

object ConsoleLogHandler {
  val logger: Logger = LoggerFactory.getLogger(ConsoleLogHandler.getClass)

  def main(args: Array[String]) {
    //val List(kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) = args.toList
    val List(kafkaZkUrl: String, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) =
      List("10.62.14.27:2181,10.62.14.10:2181,10.62.14.28:2181", "containerlog", "10", "4", "hdfs://10.62.14.46:9000/tmp/containerlog1")
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.62.14.25:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", kafkaZkUrl)
    properties.setProperty("group.id", "test")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer = new FlinkKafkaConsumer08[String](topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaConsumer)
    val dockerLogTimePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})")
    val logStream = stream.filter(_.nonEmpty)
    logStream.addSink(new SinkFunction[String] {
      override def invoke(msg: String): Unit = {
        try {
          val json: Map[String, Any] = JSON.parseFull(msg).get.asInstanceOf[Map[String, Any]]
          val rawMsg = json.getOrElse("message", "").asInstanceOf[String]
          val time = JSON.parseFull(rawMsg).get.asInstanceOf[Map[String, Any]].getOrElse("time","").asInstanceOf[String]
          val matcher = dockerLogTimePattern.matcher(time)
          var date: Date = null
          if (matcher.find) {
            val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z")
            try
              date = sdf.parse(matcher.group(1) + " UTC")
            catch {
              case e: ParseException =>
                throw new RuntimeException(e)
            }
          }
          if (date == null) date = new Date
          val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(date)
          if (!json.getOrElse("stack", "").asInstanceOf[String].isEmpty) {
            val filePath = new StringBuilder().append(outputPath).append("/")
              .append("year=").append(dateStr.substring(0, 4)).append("/")
              .append("month=").append(dateStr.substring(5, 7)).append("/")
              .append("stack=").append(json.getOrElse("stack", "").asInstanceOf[String]).append("/")
              .append("service=").append(json.getOrElse("service", "").asInstanceOf[String]).append("/")
              .append(json.getOrElse("service", "").asInstanceOf[String]).append("-").append(json.getOrElse("index", "").asInstanceOf[String])
              .append(".").append("console.out").append(".").append(dateStr).toString
            CachedDataFileWriter.write(rawMsg, filePath)
          }
        } catch {
          case e: Throwable => logger.error(msg, e)
        }
      }

      })

    try {
      env.execute("containerlog2hdfs")
    } catch {
      case e: Exception =>logger.error(e.getMessage, e)

    }


  }

}
