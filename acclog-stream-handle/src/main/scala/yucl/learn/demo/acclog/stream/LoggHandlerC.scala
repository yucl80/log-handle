package yucl.learn.demo.acclog.stream

import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by YuChunlei on 2017/5/27.
  */
object LoggHandlerC {

  case class DA(urlSet: util.TreeSet[UrlTime])


  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.62.14.49:9092")
    properties.setProperty("zookeeper.connect", "10.62.14.53:2181,10.62.14.62:2181,10.62.14.64:2181")
    properties.setProperty("group.id", "yucl-test")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE)

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("parsed-acclog", new SimpleStringSchema, properties))
    val data = stream.map(AccLog(_)).filter(x => x != None).map(_.get)
      .filter(_.stack.equals("it-dev-20170810"))
      .filter(_.service.equalsIgnoreCase("um"))

    val timedData = data.assignTimestampsAndWatermarks(new WG2())
    val keyedStream = timedData.keyBy("stack", "service")
    val windowedData = keyedStream
      .window(TumblingEventTimeWindows.of(Time.minutes(2)))

    val result2 = windowedData.aggregate(new AggregateFunction[AccLog, AccCountAccumulator, AccCountAccumulator] {
      override def createAccumulator = new AccCountAccumulator(
        null, null,
        0, 0, 0, 0, 0, 0,
        new mutable.HashSet[String], new mutable.HashSet[String],
        new mutable.HashSet[String], new mutable.TreeSet[UriTime])

      override def merge(a: AccCountAccumulator, b: AccCountAccumulator): AccCountAccumulator = {
        println("call merge")
        a.countAll += b.countAll
        a.count2xx += b.count2xx
        a.count3xx += b.count3xx
        a.count4xx += b.count4xx
        a.count5xx += b.count5xx
        a.sumBytes += b.sumBytes
        a.clientSet ++= b.clientSet
        a.sessionSet ++= b.sessionSet
        a.ipSet ++= b.ipSet
        a
      }

      override def add(log: AccLog, acc: AccCountAccumulator): Unit = {
        acc.countAll += 1
        acc.count2xx += (if (log.status.toString.startsWith("2")) 1 else 0)
        acc.count3xx += (if (log.status.toString.startsWith("3")) 1 else 0)
        acc.count4xx += (if (log.status.toString.startsWith("4")) 1 else 0)
        acc.count5xx += (if (log.status.toString.startsWith("5")) 1 else 0)
        acc.sumBytes += log.bytes

        if (!log.clientid.equals("-")) acc.clientSet += log.clientid
        if (!log.sessionid.equals("-")) acc.sessionSet += log.sessionid
        if (!log.clientip.equals("-")) acc.ipSet += log.clientip
        aggregate(acc.urlSet, log)

      }

      override def getResult(acc: AccCountAccumulator): AccCountAccumulator = acc
    }, (key: Tuple, window: TimeWindow, input: Iterable[AccCountAccumulator], out: Collector[String]) => {
      input.foreach(ca => {
        ca.stack = key.getField(0)
        ca.service = key.getField(1)
        out.collect(ca.toString)
      })
      if (input.size > 1)
        println(s"${window.getStart}-${window.getEnd}:${key}:${input.size}")
    })

    result2.print()


    try {
      env.execute("test")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def aggregate(urlSet: mutable.TreeSet[UriTime], x: AccLog): Unit = {
    val topN = 20
    val vs = urlSet.filter(u => u.uri == x.uri)
    if (vs.size > 0) {
      val f = vs.last
      if (f.time < x.time) {
        urlSet -= vs.last
        urlSet += new UriTime(x.uri, x.time)
      }
    } else {
      if (urlSet.size <= topN) {
        urlSet += new UriTime(x.uri, x.time)
      } else {
        val last = urlSet.last
        if (x.time > last.time) {
          urlSet += new UriTime(x.uri, x.time)
          urlSet -= urlSet.last
        }
      }
    }

  }


  class WG2 extends AssignerWithPeriodicWatermarks[AccLog] with Serializable {
    val maxOutOfOrderness = 5000 // `1 seconds
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



