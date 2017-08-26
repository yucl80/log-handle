package yucl.learn.demo.acclog.stream

import java.util.Properties

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

object HttpSessionCounter {

  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.62.14.49:9092")
    properties.setProperty("group.id", "yucl-test")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("parsed-acclog", new SimpleStringSchema, properties))
    val data = stream.map(AccLog(_)).filter(x => x != None).map(_.get)
      .filter(_.stack.equals("it-dev-20170810"))
      //.filter(_.service.equalsIgnoreCase("um"))
      .filter(_.status == 200)

   val windowStream = data.assignTimestampsAndWatermarks(new WatermarkGenerator)
      .keyBy("service", "sessionid")
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
      .allowedLateness(Time.seconds(1))


    /*windowStream.aggregate(new AggregateFunction[AccLog, AverageAccumulator, AverageAccumulator] {
        override def createAccumulator = new AverageAccumulator

        override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = {
          print("do merge")
          a.count += b.count
          a.sum += b.sum
          a
        }

        override def add(value: AccLog, acc: AverageAccumulator): Unit = {
          //println(value)
          acc.sum += value.time
          acc.count += 1
        }

        override def getResult(acc: AverageAccumulator): AverageAccumulator = acc
      }, (key: Tuple, window: TimeWindow, input: Iterable[AverageAccumulator], out: Collector[Double]) => {
        var count = 0l
        input.foreach(x => {
          count += x.count
        })
        printf("%s %s %s %s \n", key, window.getStart, window.getEnd, count)

      })
*/
 /*   data.assignAscendingTimestamps(_.timestamp)
      .keyBy("stack","service", "sessionid")
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply((key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[AccLog]) => {

      printf("%s-%s: %s ;%s \n",window.getStart,window.getEnd,key,input.size)
    }
    )*/

    /* windowStream.apply(new WindowFunction[AccLog, AccLog, Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[AccLog]): Unit = {
          input.foreach(x => {
            //printf("%s-%s: %s ;%s \n",window.getStart,window.getEnd,key,x)
            out.collect(x)
          })
        }
      })*/

   windowStream.apply((key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[AccLog]) => {
      input.foreach(x => {
        //printf("%s-%s: %s ;%s \n",window.getStart,window.getEnd,key,x)
        out.collect(x)
      })
    }
    )


    try {
      //env.setParallelism(2)

      env.execute("yucl-test")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  class WatermarkGenerator extends AssignerWithPeriodicWatermarks[AccLog] {
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

}
