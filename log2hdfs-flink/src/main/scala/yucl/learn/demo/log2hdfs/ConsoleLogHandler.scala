package yucl.learn.demo.log2hdfs

import java.text.{ParseException, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Date, Properties}

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

object ConsoleLogHandler {
  val logger: Logger = LoggerFactory.getLogger(ConsoleLogHandler.getClass)

  def main(args: Array[String]) {
    val List(bootstrap, topic, consumerGroup, outputPath) = args.toList
    //val List(bootstrap, topic, consumerGroup, outputPath) =
    //List("10.62.14.55:9092","containerlog","test","hdfs://10.62.14.46:9000/tmp/applog1")
    val properties = new Properties
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", consumerGroup)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaConsumer)
    val dockerLogTimePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})")
    val logStream = stream.filter(_.nonEmpty)
    logStream.addSink(new SinkFunction[String] {
      override def invoke(msg: String): Unit = {
        try {
          println(msg)
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
