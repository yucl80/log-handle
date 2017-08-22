package yucl.learn.demo.acclog.stream

import java.time.Instant
import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.util.parsing.json.JSON

object HttpSessionCounter {

  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.62.14.49:9092")
    properties.setProperty("group.id", "test2")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("parsed-acclog", new SimpleStringSchema, properties))
    val data = stream.map(new MapFunction[String, AccLog] {
      override def map(value: String): AccLog = {
        try {
          val json = JSON.parseFull(value).get.asInstanceOf[Map[String, Any]]
          val timestamp = Instant.parse(json.getOrElse("@timestamp", "").asInstanceOf[String]).toEpochMilli
          new AccLog(
            json.getOrElse("service", "").asInstanceOf[String],
            json.getOrElse("sessionid", "").asInstanceOf[String],
            json.getOrElse("clientip", "").asInstanceOf[String],
            json.getOrElse("response", 0d).asInstanceOf[Double].toInt,
            json.getOrElse("bytes", 0d).asInstanceOf[Double].toInt,
            json.getOrElse("time", 0d).asInstanceOf[Double].toInt,
            timestamp,
            1,
            json.getOrElse("message", 0d).asInstanceOf[String]
          )
        } catch {
          case e: Exception => {
            println("parse to json failed: " + value)
            e.printStackTrace()
          }
            new AccLog("", "", "", 0, 0, 0, 0, 0, "")
        }
      }
    }).filter(_.timestamp != 0).filter(_.status == 200)

    val time = data.assignAscendingTimestamps(_.timestamp)
      .keyBy("service")
      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))

      .sum("time")
    time.print()

    /*data.keyBy("service")
      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
      .apply(new WindowFunction[AccLog,AccLog,Tuple,TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[AccLog]): Unit = {

          input.foreach(x => {
            out.collect(x)
          })
        }
      }).print()*/

    try {
      //env.setParallelism(2)
      env.execute("key words alert")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}
