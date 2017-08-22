package yucl.learn.demo.acclog.stream

import java.sql.Timestamp

import scala.collection.mutable

/**
  * Created by YuChunlei on 2017/5/27.
  */
case class AccLog(service: String, sessionid: String, clientip: String, status: Int, bytes: Int, time: Int, timestamp: Long, count: Int, uri: String)


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
