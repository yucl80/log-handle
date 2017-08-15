package yucl.learn.demo.log2hdfs

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.FSDataOutputStream

/**
  * Created by yuchunlei on 2017/2/28.
  */
class CachedWriterEntity(var dataFileWriter: DataFileWriter[GenericRecord], var fsDataOutputStream: FSDataOutputStream) {
  var lastWriteTime: Long = 0l
  var needSyncDFS: Boolean = false
  var isNewFile = true

}
