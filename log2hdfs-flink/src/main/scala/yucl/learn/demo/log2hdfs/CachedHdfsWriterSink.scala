package yucl.learn.demo.log2hdfs

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory

class CachedHdfsWriterSink extends RichSinkFunction[(String, String)] {
  val logger = LoggerFactory.getLogger(ConsoleLogHandler.getClass)

  override def close() = {
    waitDataHandleComplete
    CachedDataFileWriter.closeAllFiles()
    super.close()
  }

  override def invoke(value: (String, String)) = {
    CachedDataFileWriter.write(value._1, value._2)
  }

  def waitDataHandleComplete: Unit = {
    try{
      Thread.sleep(1000)
    }catch {
      case e:InterruptedException => logger.error("sleep InterruptedException")
    }
  }

}
