package yucl.learn.demo.log2hdfs

import org.apache.avro.generic.GenericRecord
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import yucl.learn.demo.log2hdfs.AccLogHandler.logger

class AvroFileWriteSink(var partitionKeys:List[String], var outputPath :String) extends RichSinkFunction[(GenericRecord,  String)]{

  override def invoke(value: (GenericRecord,  String)) = {
    CachedAvroFileWriter.write(value._1, partitionKeys, outputPath,  value._2)
  }

  def waitDataHandleComplete: Unit = {
    try{
      Thread.sleep(1000)
    }catch {
      case e:InterruptedException => logger.error("wait Data Handle Complete sleep InterruptedException")
    }
  }

  override def close() = {
    waitDataHandleComplete
    CachedAvroFileWriter.closeAllFiles()
    super.close()
  }
}

