package yucl.learn.demo.acclog.stream

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer08}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by YuChunlei on 2017/5/27.
  */
object LoggHandler {


  def aggregate(s: Rst, x: AccLog): Unit = {
    s.count += 1
    s.bytes += x.bytes
    s.sessionSet += x.sessionid
    s.ipSet += x.clientip
    val topN = 10

    if (s.urlSet != null) {
      val vs = s.urlSet.filter(u => u.uri == x.uri)
      if (vs.size > 0) {
        val f = vs.last
        if (f.time < x.time) {
          s.urlSet -= vs.last
          s.urlSet += new UrlTime(x.uri, x.time)
        }
      } else {
        if (s.urlSet.size <= topN) {
          s.urlSet += new UrlTime(x.uri, x.time)
        } else {
          val last = s.urlSet.last
          if (x.time > last.time) {
            s.urlSet += new UrlTime(x.uri, x.time)
            s.urlSet -= s.urlSet.last
          }
        }
      }
    }


  }

  def main(args: Array[String]) {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.21.27:9092")
    properties.setProperty("zookeeper.connect", "192.168.21.12:2181,192.168.21.13:2181,192.168.21.14:2181")
    properties.setProperty("group.id", "key-words-alert")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   env.enableCheckpointing(10000,CheckpointingMode.AT_LEAST_ONCE)

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("parsed-acclog", new SimpleStringSchema, properties))
    val data = stream.map( AccLog(_)).filter(x => x != None).map(_.get)
    val timedData = data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[AccLog] {
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
    })
    val keyedStream = timedData.keyBy(0)
    val windowedData = keyedStream
      .window(TumblingEventTimeWindows.of(Time.minutes(2)))


    /* val result1 = windowedData.fold(new Rst(0,0d,new mutable.HashSet[String](),new mutable.HashSet[String](),0,new mutable.TreeSet[UrlTime]()),
         (s: Rst, x: AccLog) => {
          aggregate(s,x)
           s
         },
         (key: Tuple, window: TimeWindow, input: Iterable[Rst],out: Collector[Result]) => {
           input.foreach(e => {
             try{
               if (e != null)
               out.collect(new Result(key.getField(0), e.count, e.bytes, e.sessionSet.size, e.ipSet.size, null,new Timestamp(window.getEnd)))
             }catch {
               case e :Exception => e.printStackTrace()
             }

           })

         }
       ).print()*/




    val resultData = windowedData.apply(new WindowFunction[AccLog, Result, Tuple, TimeWindow] {
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[Result]): Unit = {
        val s = new Rst(new mutable.TreeSet[UrlTime](),0, 0, new mutable.HashSet[String](), new mutable.HashSet[String](), 0)
        input.foreach(x => {
          try {
            aggregate(s, x)
          } catch {
            case e: Throwable => e.printStackTrace()
          }

        })
        try {
          println(s.urlSet)
          val r = new Result(key.getField(0), s.count, s.bytes, s.sessionSet.size, s.ipSet.size, s.urlSet, new Timestamp(window.getEnd))
          out.collect(r)
        } catch {
          case e: Throwable => e.printStackTrace()
        }


      }
    })

    resultData.print()

    /*keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(60)))
    .apply(new WindowFunction[AccLog, (String,Long), Tuple, TimeWindow] {
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[(String,Long)]): Unit = {
        var sessionSet = new mutable.HashSet[String]()
        input.foreach(e => {
          sessionSet += e.sessionid
        })
        println(new Timestamp(window.getStart),new Timestamp(window.getEnd))
        out.collect(key.getField(0),sessionSet.size)
      }
    }).print()*/


    /*windowedData.fold(new mutable.HashSet[String](),
      (s:mutable.HashSet[String], a:AccLog)=>{ s += a.sessionid},
      (key: Tuple, window: TimeWindow, input: Iterable[mutable.HashSet[String]], out: Collector[mutable.HashSet[String]]) =>{
         input.foreach(e => {
            out.collect(e)
         })
      }
    )*/

    /* val resultData = windowedData.apply(new WindowFunction[AccLog, Result, Tuple, TimeWindow] {
       override def apply(key: Tuple, window: TimeWindow, input: Iterable[AccLog], out: Collector[Result]): Unit = {
         val map = new mutable.HashMap[String, Result]()
         val sessionMap = new mutable.HashMap[String, mutable.HashSet[String]]()
         val ipMap = new mutable.HashMap[String, mutable.HashSet[String]]()
         val topUrlMap = new mutable.HashMap[String, mutable.TreeSet[UrlTime]]
         val topN = 10
         input.foreach(x => {
           val r = map.getOrElse(x.system, new Result(x.system, 0, 0d, 0, 0, null))
           r.count += 1
           r.bytes += x.bytes
           map.put(x.system, r)

           val sessionSet = sessionMap.getOrElse(x.system, new mutable.HashSet[String]())
           sessionSet += x.sessionid
           sessionMap.put(x.system, sessionSet)

           val ipSet = ipMap.getOrElse(x.system, new mutable.HashSet[String]())
           ipSet += x.clientip
           ipMap.put(x.system, ipSet)

           val urlSet = topUrlMap.getOrElse(x.system, new mutable.TreeSet[UrlTime])
           val v = urlSet.filter(u => u.url == x.uri && u.time < x.time).last
           if (v != null) {
             urlSet -= v
             urlSet += new UrlTime(x.uri, x.time)
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
         })
         map.foreach(x => {
           x._2.sessionCount = sessionMap.get(x._1).get.size
           x._2.ipCount = ipMap.get(x._1).get.size
           x._2.topUrl = topUrlMap.get(x._1).get
           out.collect(x._2)
         })
       }
     })
 */

    // resultData.print.setParallelism(6)

    /*    val count = timedData.keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .sum("count").map(x => new Tuple3[String, String, Int](x.system, "count", x.count))
        val maxTime = timedData.keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .max("time").map(x => new Tuple3[String, String, Double](x.system, "maxtime", x.time))
        val sumBytes = timedData.keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .sum("bytes").map(x => new Tuple3[String, String, Double](x.system, "sumBytes", x.bytes))
        var joinedStreams = count.join(maxTime).where(x => x._1).equalTo(x => x._1)
          .window(TumblingEventTimeWindows.of(Time.seconds(300)))
          .apply(new JoinFunction[(String, String, Int), (String, String, Double), (String, Int, Double)] {
            override def join(first: (String, String, Int), second: (String, String, Double)): (String, Int, Double) = {
              (first._1, first._3, second._3)
            }
          }).print()

        sumBytes.addSink(new SinkFunction[(String, String, Double)]() {
          override def invoke(value: (String, String, Double)): Unit = {

          }
        })*/



    try {
      env.setParallelism(2)
      env.execute("test")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}



