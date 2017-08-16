package yucl.learn.demo.log2hdfs

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date
import java.util.regex.Pattern

import com.jayway.jsonpath.JsonPath
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object ConsoleLog {
  val logger: Logger = LoggerFactory.getLogger(ConsoleLog.getClass)

  def main(args: Array[String]) {
    //val List(kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) = args.toList
    val List(kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) =
    List("10.62.14.27:2181,10.62.14.10:2181,10.62.14.28:2181", "containerlog", "10", "4", "hdfs://10.62.14.46:9000/tmp/consoleout")

    val partitionKeys = List("year", "month", "stack", "service")
    val sparkConf = new SparkConf()
      .setMaster("local[8]").setAppName("log2hdfs-test")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val dockerLogTimePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})")
    val WINDOW_SIZE = Seconds(1)
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, WINDOW_SIZE)
    val pattern: Pattern = Pattern.compile("^\\[\\d{2}/\\d{2} ")
    val fullDatePattern: Pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}) ")
    val kafkaStreams = (1 to kafkaStreamCount.toInt).map { _ =>
      KafkaUtils.createStream(
        streamingContext, kafkaZkUrl, sparkConf.get("name", "log2hdfs-test"), Map(topic -> kafkaConsumerThreadCount.toInt),StorageLevel.MEMORY_AND_DISK_SER )
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
              val jsonContext = JsonPath.parse(rawMsg)
              val time = jsonContext.read("$.time", classOf[String])
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
              if (!json.read("$.stack", classOf[String]).isEmpty) {
                val filePath = new StringBuilder().append(outputPath).append("/")
                  .append("year=").append(dateStr.substring(0, 4)).append("/")
                  .append("month=").append(dateStr.substring(5, 7)).append("/")
                  .append("stack=").append(json.read("$.stack", classOf[String])).append("/")
                  .append("service=").append(json.read("$.service", classOf[String])).append("/")
                  .append(json.read("$.service", classOf[String])).append("-").append(json.read("$.index", classOf[String]))
                  .append(".").append("console.out").append(".").append(dateStr).toString
                CachedDataFileWriter.write(rawMsg, filePath, conf)
              }
            } catch {
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
