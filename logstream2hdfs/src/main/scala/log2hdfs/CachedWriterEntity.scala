package yucl.learn.demo.log2hdfs


import java.io.BufferedWriter

import org.apache.hadoop.fs.FSDataOutputStream

/**
  * Created by yuchunlei on 2017/2/28.
  */
class CachedWriterEntity(var dataFileWriter:BufferedWriter, var fsDataOutputStream: FSDataOutputStream) {
  var lastWriteTime: Long = 0l
  var needSyncDFS: Boolean = false
}
