package yucl.learn.demo.acclog.stream

import java.util
import java.util.Properties

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by YuChunlei on 2017/5/27.
  */
object LoggHandlerB {
  case class DA( urlSet : util.TreeSet[UrlTime])

  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.62.14.49:9092")
    //properties.setProperty("zookeeper.connect", "192.168.21.12:2181,192.168.21.13:2181,192.168.21.14:2181")
    properties.setProperty("group.id", "yucl-test")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE)

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("parsed-acclog", new SimpleStringSchema, properties))
    val data = stream.map(AccLog(_)).filter(x => x != None).map(_.get)
    val timedData = data.assignTimestampsAndWatermarks(new WG())
    val keyedStream = timedData.keyBy(0)
    val windowedData = keyedStream
      .window(TumblingEventTimeWindows.of(Time.minutes(2)))



    val result1 = windowedData.fold(new mutable.TreeSet[UrlTime](),
      (s: mutable.TreeSet[UrlTime], x: AccLog) => {
        aggregate(s, x)
        s
      },
      (key: Tuple, window: TimeWindow, input: Iterable[mutable.TreeSet[UrlTime]], out: Collector[Result]) => {
        input.foreach(e => {
          try {

             // out.collect(new Result(key.getField(0), 0,0,0,0, null, new Timestamp(window.getEnd)))
          } catch {
            case e: Exception => e.printStackTrace()
          }
        })
      }
    ).print()

    try {
      env.setParallelism(2)
      env.execute("test")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def aggregate(urlSet: mutable.TreeSet[UrlTime], x: AccLog): Unit = {
  /*  s.count += 1
    s.bytes += x.bytes
    s.sessionSet += x.sessionid
    s.ipSet += x.clientip*/
    val topN = 10
    if (urlSet != null) {
      if (urlSet != null) {
        val vs = urlSet.filter(u => u.uri == x.uri)
        if (vs.size > 0) {
          val f = vs.last
          if (f.time < x.time) {
            urlSet -= vs.last
            urlSet += new UrlTime(x.uri, x.time)
          }
        } else {
          if (urlSet.size <= topN) {
            urlSet += new UrlTime(x.uri, x.time)
          } else {
            val last = urlSet.last
            if (x.time > last.time) {
              urlSet += new UrlTime(x.uri, x.time)
              urlSet -= urlSet.last
            }
          }
        }
      }
    }


  }

  class WG extends  AssignerWithPeriodicWatermarks[AccLog] with Serializable{
    val maxOutOfOrderness = 5000 // 3.5 seconds
    var currentMaxTimestamp = 0L

    override def extractTimestamp(element: AccLog, previousElementTimestamp: Long): Long = {
      val timestamp: Long = element.timestamp
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      timestamp
    }

    override def getCurrentWatermark: Watermark = {
      return new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }
  }

}



