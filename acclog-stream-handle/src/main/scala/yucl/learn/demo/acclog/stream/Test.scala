package yucl.learn.demo.acclog.stream

import java.time.ZoneId
import java.util.Date

/**
  * Created by YuChunlei on 2017/5/27.
  */
object Test {

  def main(args: Array[String]): Unit = {
    //20/Aug/2017:17:35:51 +0800
    val timestamp = "2017-08-02T16:35:51.000Z"
    import java.time.Instant
    val instant = Instant.parse(timestamp)
    //val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    //val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.XXXZ")
    //val tt = sdf.parse(timestamp).getTime

    import java.time.format.DateTimeFormatter
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val gg = instant.atZone(ZoneId.of("Asia/Shanghai"))
    println(gg.format(formatter).toString)
    println(gg.getMonthValue)
    println(gg.getYear)
    println(gg.getDayOfMonth)
    println(("02").toInt)
    println(new Date(1503715908000l))


    println(new Date(instant.toEpochMilli))
  }

}
