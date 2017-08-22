package yucl.learn.demo.log2hdfs

import java.text.SimpleDateFormat

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, _}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.kafka.KafkaUtils

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON.parseFull

object AccLogHandler {
  val logger: Logger = LoggerFactory.getLogger(AccLogHandler.getClass)

  def main(args: Array[String]) {
    val List(kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) = args.toList
    //val List(appName, kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount, outputPath) = List("AccLogSaver",
     // "10.62.14.27:2181,10.62.14.10:2181,10.62.14.28:2181", "parsed-acclog", "10", "1", "hdfs://10.62.14.46:9000/applogs/acclog")

    val partitionKeys = List("year", "month","stack", "service")
    val sparkConf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val WINDOW_SIZE = Seconds(1)
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, WINDOW_SIZE)
    val kafkaStreams = (1 to kafkaStreamCount.toInt).map { _ =>
      KafkaUtils.createStream(
        streamingContext, kafkaZkUrl, sparkConf.get("name","acclog2hdfs"), Map(topic -> kafkaConsumerThreadCount.toInt))
    }
    val unifiedStream = streamingContext.union(kafkaStreams)
    val accLogs = unifiedStream
      .map(_._2)
      .map(parseFull(_))
      .map(_.getOrElse(Map()))
      .map(_.asInstanceOf[Map[String, Any]])
      .filter(_.nonEmpty)
      //.filter(_.contains("message"))
      .filter(_.contains("@timestamp"))
      .filter(_.contains("service"))

    accLogs.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        var schema = CachedAvroFileWriter.schema
        if (schema == null) {
          val schemaInputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("acclog.avsc")
          CachedAvroFileWriter.schema = new Schema.Parser().parse(schemaInputStream)
          schemaInputStream.close()
          schema = CachedAvroFileWriter.schema
        }
        val conf = new Configuration()
        val fields = List("service", "instance", "@timestamp", "uri", "query", "time", "bytes", "response", "verb", "path", "sessionid", "auth", "agent", "host", "ip", "clientip", "xforwardedfor", "thread", "uidcookie", "referrer", "message","stack")
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        partition.foreach(log => {
          try {
            val record: GenericRecord = new GenericData.Record(schema)
            val Array(year, month) = log.getOrElse("@timestamp", "").asInstanceOf[String].split("-").slice(0, 2)
            record.put("year", year.toInt)
            record.put("month", month.toInt)
            val time = log.getOrElse("time", 0L) match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => null
            }
            val bytes = log.getOrElse("bytes", 0L) match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => null
            }
            val response = log.getOrElse("response", 200) match {
              case i: Integer => i
              case l: Long => l.toInt
              case _ => null
            }
            record.put(fields.head, log.getOrElse(fields.head, "").asInstanceOf[String])
            record.put(fields(1), log.getOrElse(fields(1), "").asInstanceOf[String])
            record.put("timestamp", simpleDateFormat.parse(log.getOrElse(fields(2), "").asInstanceOf[String]).getTime)
            record.put(fields(3), log.getOrElse(fields(3), "").asInstanceOf[String])
            record.put(fields(4), log.getOrElse(fields(4), "").asInstanceOf[String])
            record.put(fields(5), time)
            record.put(fields(6), bytes)
            record.put(fields(7), response)
            record.put(fields(8), log.getOrElse(fields(8), "").asInstanceOf[String])
            record.put(fields(9), log.getOrElse(fields(9), "").asInstanceOf[String])
            record.put(fields(10), log.getOrElse(fields(10), "").asInstanceOf[String])
            record.put(fields(11), log.getOrElse(fields(11), "").asInstanceOf[String])
            record.put(fields(12), log.getOrElse(fields(12), "").asInstanceOf[String])
            record.put(fields(13), log.getOrElse(fields(13), "").asInstanceOf[String])
            record.put(fields(14), log.getOrElse(fields(14), "").asInstanceOf[String])
            record.put(fields(15), log.getOrElse(fields(15), "").asInstanceOf[String])
            record.put(fields(16), log.getOrElse(fields(16), "").asInstanceOf[String])
            record.put(fields(17), log.getOrElse(fields(17), "").asInstanceOf[String])
            record.put(fields(18), log.getOrElse(fields(18), "").asInstanceOf[String])
            record.put(fields(19), log.getOrElse(fields(19), "").asInstanceOf[String])
            record.put(fields(20), log.getOrElse(fields(20), "").asInstanceOf[String])
            record.put(fields(21), log.getOrElse(fields(21), "").asInstanceOf[String])
            logger.debug(record.toString)
            CachedAvroFileWriter.write(record, partitionKeys, outputPath, schema, conf)
          } catch {
            case e: Throwable => logger.error(log.toString(), e)
          }
        })
      })
    })

    /*sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        super.onApplicationEnd(applicationEnd)
        logger.error("wait 1000ms to close files")
        Thread.sleep(1000)
        CachedDataFileWriter.closeAllFiles()
      }
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        super.onJobEnd(jobEnd)
        logger.error("wait 1000ms to close files")
        Thread.sleep(1000)
        CachedDataFileWriter.syncAllDFS()
      }
    })*/
    streamingContext.start()
    streamingContext.awaitTermination()


  }

}
