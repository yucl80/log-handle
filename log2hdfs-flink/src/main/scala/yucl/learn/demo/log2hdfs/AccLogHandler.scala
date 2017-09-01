package yucl.learn.demo.log2hdfs

import java.time.{Instant, ZoneId}
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON.parseFull

object AccLogHandler {
  val logger: Logger = LoggerFactory.getLogger(AccLogHandler.getClass)

  def main(args: Array[String]) {
    val List(bootstrap, topic, consumerGroup, outputPath) = args.toList
    val partitionKeys = List("year", "month", "stack", "service")
    val properties = new Properties
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", consumerGroup)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaConsumer)
    val fields = List("service", "instance", "@timestamp", "uri", "query", "time", "bytes", "response", "verb", "path", "sessionid", "auth", "agent", "host", "ip", "clientip", "xforwardedfor", "thread", "uidcookie", "referrer", "message", "stack")

    val accLogs = stream
      .map(parseFull(_))
      .map(_.getOrElse(Map()))
      .map(_.asInstanceOf[Map[String, Any]])
      .filter(_.nonEmpty)
      .filter(_.contains("service"))

    accLogs.addSink((log: Map[String,Any])=> {
      try {
        var schema = CachedAvroFileWriter.schema
        if (schema == null) {
          val schemaInputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("acclog.avsc")
          CachedAvroFileWriter.schema = new Schema.Parser().parse(schemaInputStream)
          schemaInputStream.close()
          schema = CachedAvroFileWriter.schema
        }
        val record: GenericRecord = new GenericData.Record(schema)
        val instant = Instant.parse(log.getOrElse("@timestamp", "").asInstanceOf[String])
        val localTime = instant.atZone(ZoneId.of("Asia/Shanghai"))
        val path = log.getOrElse("path","").asInstanceOf[String]
        val fileBaseName = new StringBuilder().append(log.getOrElse("service","").asInstanceOf[String]).append("-")
            .append(log.getOrElse("index","").asInstanceOf[String]).append(".")
            .append(path.substring(path.lastIndexOf("/")+1)).toString()
        record.put("year", localTime.getYear)
        record.put("month", localTime.getMonthValue)
        val time = log.getOrElse("time", 0L) match {
          case d: Double => d.toLong
          case i: Integer => i.toLong
          case l: Long => l
          case _ => null
        }
        val bytes = log.getOrElse("bytes", 0L) match {
          case d: Double => d.toLong
          case i: Integer => i.toLong
          case l: Long => l
          case _ => null
        }
        val response = log.getOrElse("response", 200) match {
          case d: Double => d.toInt
          case i: Integer => i
          case l: Long => l.toInt
          case _ => null
        }
        record.put(fields.head, log.getOrElse(fields.head, "").asInstanceOf[String])
        record.put(fields(1), log.getOrElse(fields(1), "").asInstanceOf[String])
        record.put("timestamp", instant.getEpochSecond)
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
        CachedAvroFileWriter.write(record, partitionKeys, outputPath, schema,fileBaseName)
      } catch {
        case e: Throwable => logger.error(log.toString(), e)
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
