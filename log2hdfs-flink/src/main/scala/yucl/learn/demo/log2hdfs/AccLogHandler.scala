package yucl.learn.demo.log2hdfs

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, _}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON.parseFull

object AccLogHandler {
  val logger: Logger = LoggerFactory.getLogger(AccLogHandler.getClass)

  def main(args: Array[String]) {
    val partitionKeys = List("year", "month", "stack", "service")
    val List(bootstrap, topic, consumerGroup, outputPath) =
      List("10.62.14.66:9092", "parsed-acclog", "test", "hdfs://10.62.14.46:9000/tmp/acclog1")
    val properties = new Properties
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", consumerGroup)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaConsumer)
    val fields = List("service", "instance", "@timestamp", "uri", "query", "time", "bytes", "response", "verb", "path", "sessionid", "auth", "agent", "host", "ip", "clientip", "xforwardedfor", "thread", "uidcookie", "referrer", "message", "stack")
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    val accLogs = stream
      .map(parseFull(_))
      .map(_.getOrElse(Map()))
      .map(_.asInstanceOf[Map[String, Any]])
      .filter(_.nonEmpty)
      .filter(_.contains("service"))

    accLogs.addSink(new SinkFunction[Map[String,Any]] {
      override def invoke(log: Map[String,Any]): Unit = {
        try {
          //val log: Map[String, Any] = JSON.parseFull(msg).get.asInstanceOf[Map[String, Any]]
          var schema = CachedAvroFileWriter.schema
          if (schema == null) {
            val schemaInputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("acclog.avsc")
            CachedAvroFileWriter.schema = new Schema.Parser().parse(schemaInputStream)
            schemaInputStream.close()
            schema = CachedAvroFileWriter.schema
          }
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
          CachedAvroFileWriter.write(record, partitionKeys, outputPath, schema)
        } catch {
          case e: Throwable => logger.error(log.toString(), e)
        }
      }
    }
    )

    try {
      env.execute("acclog2hdfs")
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }

  }

}
