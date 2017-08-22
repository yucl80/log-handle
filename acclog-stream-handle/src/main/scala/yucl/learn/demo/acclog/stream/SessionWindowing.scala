package yucl.learn.demo.acclog.stream

import java.util

import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * An example of session windowing that keys events by ID and groups and counts them in
  * session with gaps of 3 milliseconds.
  */
object SessionWindowing {
  @SuppressWarnings(Array("serial"))
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val fileOutput = params.has("output")
    val input = new util.ArrayList[Tuple3[String, Long, Integer]]
    input.add(new Tuple3[String, Long, Integer]("a", 1L, 1))
    input.add(new Tuple3[String, Long, Integer]("b", 1L, 1))
    input.add(new Tuple3[String, Long, Integer]("b", 3L, 1))
    input.add(new Tuple3[String, Long, Integer]("b", 5L, 1))
    input.add(new Tuple3[String, Long, Integer]("c", 6L, 1))
    // We expect to detect the session "a" earlier than this point (the old
    // functionality can only detect here when the next starts)
    input.add(new Tuple3[String, Long, Integer]("a", 10L, 1))
    // We expect to detect session "b" and "c" at this point as well
    input.add(new Tuple3[String, Long, Integer]("c", 11L, 1))
    val source = env.addSource(new SourceFunction[Tuple3[String, Long, Integer]]() {
      @throws[Exception]
      override def run(ctx: SourceFunction.SourceContext[Tuple3[String, Long, Integer]]): Unit = {
        import scala.collection.JavaConversions._
        for (value <- input) {
          ctx.collectWithTimestamp(value, value.f1)
          ctx.emitWatermark(new Watermark(value.f1 - 1))
        }
        ctx.emitWatermark(new Watermark(Long.MaxValue))
      }

      override

      def cancel(): Unit = {
      }
    })
    // We create sessions for each id with max timeout of 3 time units
    val aggregated = source.keyBy(0).window(EventTimeSessionWindows.withGap(Time.milliseconds(3L))).sum(2)
    if (fileOutput) aggregated.writeAsText(params.get("output"))
    else {
      System.out.println("Printing result to stdout. Use --output to specify output path.")
      aggregated.print
    }
    env.execute
  }
}