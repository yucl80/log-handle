package yucl.learn.demo.log2hdfs

import org.apache.avro.generic.GenericRecord
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class AvroFileWriteSink(var partitionKeys:List[String], var outputPath :String) extends RichSinkFunction[(GenericRecord,  String)]{

  override def invoke(value: (GenericRecord,  String)) = {
    CachedAvroFileWriter.write(value._1, partitionKeys, outputPath,  value._2)
  }

  override def close() = {
    CachedAvroFileWriter.closeAllFiles()
    super.close()
  }
}

