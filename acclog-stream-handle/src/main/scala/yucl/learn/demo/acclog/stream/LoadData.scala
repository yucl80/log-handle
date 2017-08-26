package yucl.learn.demo.acclog.stream

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

object LoadData {

  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.62.14.66:9092")
    properties.setProperty("zookeeper.connect", "10.62.14.53:2181,10.62.14.62:2181,10.62.14.64:2181")
    properties.setProperty("group.id", "yucl-test1")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("parsed-acclog", new SimpleStringSchema, properties))
    val data = stream.map(AccLog(_)).filter(x => x != None).map(_.get)
      .filter(!_.sessionid.equalsIgnoreCase("-"))
    // .filter(_.stack.equals("it-dev-20170810"))
    //.filter(_.service.equalsIgnoreCase("um"))
    //.filter(_.clientid.equalsIgnoreCase("yucl@cmrh.com"))

    val windowStream = data
      .assignTimestampsAndWatermarks(new WatermarkGeneratorX)
      .keyBy("stack", "service", "sessionid")
      .window(EventTimeSessionWindows.withGap(Time.seconds(300)))
      .allowedLateness(Time.seconds(5))
    val result = windowStream.aggregate(new AggregateFunction[AccLog, CountAccumulator, CountAccumulator] {
      override def createAccumulator = new CountAccumulator()

      override def merge(a: CountAccumulator, b: CountAccumulator): CountAccumulator = {
        a.count += b.count
        a.sum += b.sum
        a
      }

      override def add(value: AccLog, acc: CountAccumulator): Unit = {
        acc.sum += value.time
        acc.count += 1
      }

      override def getResult(acc: CountAccumulator): CountAccumulator = acc
    }, (key: Tuple, window: TimeWindow, input: Iterable[CountAccumulator], out: Collector[String]) => {
      input.foreach(ca => {
        if (ca.count > 1)
          out.collect(s"***${window.getStart}-${window.getEnd}:${key}:${ca.count}")
      })
      if (input.size > 1)
        println(s"${window.getStart}-${window.getEnd}:${key}:${input.size}")
    })

    result.print()

    try {
      env.execute("test")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  class WatermarkGeneratorX extends AssignerWithPeriodicWatermarks[AccLog] with Serializable {
    var ts = Long.MinValue

    override def getCurrentWatermark: Watermark = {
      new Watermark(ts)
    }

    override def extractTimestamp(t: AccLog, l: Long): Long = {
      //printf("%s %s \n",t.timestamp,l)
      ts = Math.max(t.timestamp, l)
      t.timestamp
    }
  }

  class CountAccumulator() {
    var count = 0
    var sum = 0

    override def toString: String = "CountAccumulator{" + "count=" + count + ", sum=" + sum + '}'
  }

}
