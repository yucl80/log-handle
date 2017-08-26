package yucl.learn.demo.acclog.stream

import java.sql.Timestamp
import java.time.Instant

import scala.collection.mutable
import scala.util.parsing.json.JSON
import scala.util.{Failure, Success, Try}

/**
  * Created by YuChunlei on 2017/5/27.
  */
case class AccLog(stack:String,service: String,clientid:String, sessionid: String, clientip: String, status: Int, bytes: Int, time: Int, timestamp: Long, uri: String,count: Int)

object AccLog {
  def apply(raw: String): Option[AccLog] = {
    Try {
      val json = JSON.parseFull(raw).get.asInstanceOf[Map[String, Any]]
      var t = json.getOrElse("@timestamp", 0d).asInstanceOf[String]
     /* val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddTHH:MI:ss.sss+Z")
      val timestamp = sdf.parse(t).getTime*/
      val timestamp = Instant.parse(json.getOrElse("@timestamp", "").asInstanceOf[String]).toEpochMilli
      AccLog(
        json.getOrElse("stack", "").asInstanceOf[String],
        json.getOrElse("service", "").asInstanceOf[String],
        json.getOrElse("auth", "").asInstanceOf[String],
        json.getOrElse("sessionid", "").asInstanceOf[String],
        json.getOrElse("clientip", "").asInstanceOf[String],
        json.getOrElse("response", 0d).asInstanceOf[Double].toInt,
        json.getOrElse("bytes", 0d).asInstanceOf[Double].toInt,
        json.getOrElse("time", 0d).asInstanceOf[Double].toInt,
        timestamp,
        json.getOrElse("uri", "").asInstanceOf[String],
        1
      )
    } match {
      case Success(e) ⇒ {
        Some(e)
      }
      case Failure(e) ⇒ {
        println("error:"+raw)
        e.printStackTrace()
        None
      }
    }
  }
}

case class UrlTime(var uri: String, var time: Double) extends Ordered[UrlTime] {
  override def toString: String = {
    uri + " :" + time
  }

  def compare(that: UrlTime) = {
    if (this.uri == that.uri)
      0
    else
      this.time.compareTo(that.time)
  }
}

case class Rst( urlSet: mutable.TreeSet[UrlTime],var count: Int, var bytes: Int, sessionSet: mutable.Set[String], ipSet: mutable.Set[String], count500: Int)

case class Result(service: String, count: Int, bytes: Int, sessionCount: Int, ipCount: Int, topUrl: mutable.Set[UrlTime], timestamp: Timestamp)
