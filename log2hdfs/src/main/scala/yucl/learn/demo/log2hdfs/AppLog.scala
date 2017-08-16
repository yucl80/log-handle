package yucl.learn.demo.log2hdfs

import java.util.Calendar
import java.util.regex.Pattern

import com.jayway.jsonpath.JsonPath
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object AppLog {
  val logger: Logger = LoggerFactory.getLogger(AppLog.getClass)


  def main(args: Array[String]) {
    val List(kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) = args.toList
    //val List(kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) =
      //List("10.62.14.27:2181,10.62.14.10:2181,10.62.14.28:2181", "applog", "10", "4", "hdfs://10.62.14.46:9000/tmp/applog")

    val partitionKeys = List("year", "month", "stack", "service")
    val sparkConf = new SparkConf()
     // .setMaster("local[8]").setAppName("log2hdfs")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val WINDOW_SIZE = Seconds(1)
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, WINDOW_SIZE)
    val pattern: Pattern = Pattern.compile("^\\[\\d{2}/\\d{2} ")
    val fullDatePattern: Pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}) ")
    val kafkaStreams = (1 to kafkaStreamCount.toInt).map { _ =>
      KafkaUtils.createStream(
        streamingContext, kafkaZkUrl, sparkConf.get("name", "log2hdfs"), Map(topic -> kafkaConsumerThreadCount.toInt))
    }
    kafkaStreams.toStream.foreach(unifiedStream => {
      val logStream = unifiedStream
        .map(_._2)
        .map(JsonPath.parse(_))
      logStream.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val conf = new Configuration()
          partition.foreach(json => {
            try {
              val rawMsg = json.read("$.message", classOf[String])
              var date = ""
              val matcher = fullDatePattern.matcher(rawMsg)
              if (matcher.find) date = matcher.group(1)
              else if (pattern.matcher(rawMsg).find) {
                val year = String.valueOf(Calendar.getInstance.get(Calendar.YEAR))
                val eventTime = year + "-" + rawMsg.substring(1, 6)
                date = eventTime.replaceAll("/", "-")
              }
              val rawPath = json.read("$.path", classOf[String])

              var fileName = rawPath.substring(rawPath.lastIndexOf('/') + 1)
              if (!fullDatePattern.matcher(fileName).find) fileName = fileName + "." + date

              val filePath = new StringBuilder().append(outputPath).append("/")
                .append("year=").append(date.substring(0, 4)).append("/")
                .append("month=").append(date.substring(5, 7)).append("/")
                .append("stack=").append(json.read("$.stack", classOf[String])).append("/")
                .append("service=").append(json.read("$.service", classOf[String])).append("/")
                .append(json.read("$.service", classOf[String])).append("-").append(json.read("$.index", classOf[String]))
                .append(".").append(fileName).toString
              CachedDataFileWriter.write(rawMsg, filePath, conf)
            }catch{
              case e: Throwable => logger.error(json.read("$.message", classOf[String]), e)
            }
          })

        })
      })

    })
    streamingContext.start()
    streamingContext.awaitTermination()


  }

}
