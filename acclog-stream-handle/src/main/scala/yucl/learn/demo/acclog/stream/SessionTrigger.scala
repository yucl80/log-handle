package yucl.learn.demo.acclog.stream


import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window

import scala.collection.mutable

class SessionTrigger[W <: Window] extends Trigger[AccLog,W] {
  val sessionSet  = mutable.HashSet[String]()

  override def onElement(element: AccLog, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    if(!sessionSet.contains(element.sessionid)){
      TriggerResult.FIRE
    }else {
      sessionSet += element.sessionid
      TriggerResult.CONTINUE
    }

  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: TriggerContext) = {

  }
}

