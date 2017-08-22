package yucl.learn.demo.log2hdfs

import java.util
import java.util.UUID
import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic._
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.hdfs.DFSOutputStream
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap

object CachedAvroFileWriter {
  val logger: Logger = LoggerFactory.getLogger(CachedAvroFileWriter.getClass)
  val fileName: String = UUID.randomUUID().toString
  private val fileCache: TrieMap[String, CachedAvroWriterEntity] = new TrieMap[String, CachedAvroWriterEntity]
  val scheduledExecutorService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build())
  var schema: Schema = null

  def write(record: GenericRecord, partitionKeys: List[String], basePath: String, schema: Schema, configuration: Configuration): Unit = {
    val fileFullName = buildFilePath(record, partitionKeys, basePath) + "/" + fileName + ".avro"
    val cacheWriterEntity = getDataFileWriter(fileFullName, schema, configuration)
    cacheWriterEntity.synchronized {
      val dataFileWriter = cacheWriterEntity.dataFileWriter
      dataFileWriter.append(record)
      cacheWriterEntity.needSyncDFS = true
      cacheWriterEntity.lastWriteTime = System.currentTimeMillis()
      dataFileWriter.flush()
      if (cacheWriterEntity.isNewFile) {
        syncDFS(cacheWriterEntity)
        cacheWriterEntity.isNewFile = false
      }
    }

  }

  def buildFilePath(record: GenericRecord, partitionKeys: List[String], basePath: String): String = {
    var filePath: String = ""
    partitionKeys.foreach(pk =>
      filePath = filePath + "/" + pk + "=" + record.get(pk))
    basePath + filePath
  }

  def getDataFileWriter(fileName: String, schema: Schema, conf: Configuration): CachedAvroWriterEntity = {
    fileCache.synchronized {
      var cacheWriterEntity: CachedAvroWriterEntity = fileCache.getOrElse(fileName, null)
      var dfw: DataFileWriter[GenericRecord] = null
      if (cacheWriterEntity == null) {
        val filePath = new Path(fileName)
        val fileSystem = filePath.getFileSystem(conf)
        val datumWriter = new SpecificDatumWriter[GenericRecord](schema)
        val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
        //dataFileWriter.setCodec(CodecFactory.snappyCodec())
        //dataFileWriter.setCodec(CodecFactory.deflateCodec(5))
        var fsDataOutputStream: FSDataOutputStream = null
        if (fileSystem.exists(filePath)) {
          fsDataOutputStream = fileSystem.append(filePath)
          dfw = dataFileWriter.appendTo(new FsInput(filePath, conf), fsDataOutputStream)
        } else {
          fsDataOutputStream = fileSystem.create(filePath, false)
          dfw = dataFileWriter.create(schema, fsDataOutputStream)
        }
        //dfw.setFlushOnEveryBlock(true)
        //dfw.setSyncInterval(1024)

        cacheWriterEntity = new CachedAvroWriterEntity(dfw, fsDataOutputStream)
        fileCache.put(fileName, cacheWriterEntity)
      }
      cacheWriterEntity
    }
  }

  def syncDFS(cachedWriterEntity: CachedAvroWriterEntity): Unit = {
    cachedWriterEntity.synchronized {
      if (cachedWriterEntity.needSyncDFS) {
        cachedWriterEntity.dataFileWriter.flush()
        val fsDataOutputStream = cachedWriterEntity.fsDataOutputStream.getWrappedStream()
        val dFSOutputStream = fsDataOutputStream.asInstanceOf[DFSOutputStream]
        dFSOutputStream.hsync(util.EnumSet.of(SyncFlag.UPDATE_LENGTH))
        cachedWriterEntity.needSyncDFS = false
      }
    }
  }

  def syncAllDFS(): Unit = {
    for ((fileName, cachedWriterEntity) <- fileCache) {
      try {
        syncDFS(cachedWriterEntity)
      }
      catch {
        case e: Exception => logger.error(fileName, e)
      }
    }
  }

  scheduledExecutorService.scheduleAtFixedRate(new Runnable {
    override def run() = {
      syncAllDFS()
    }
  }, 30, 30, TimeUnit.SECONDS)

  def closeTimeoutFiles(): Unit = {
    for ((fileName, cachedWriterEntity) <- fileCache) {
      if (System.currentTimeMillis() - cachedWriterEntity.lastWriteTime > 5 * 24 * 60 * 60 * 1000) {
        try {
          fileCache.remove(fileName)
          syncDFS(cachedWriterEntity)
          cachedWriterEntity.dataFileWriter.close()
          cachedWriterEntity.fsDataOutputStream.close()
          logger.info(fileName + " remove from writer cache")
        } catch {
          case e: Exception => logger.error(fileName, e)
        }
      }
    }
  }


  scheduledExecutorService.scheduleWithFixedDelay(new Runnable {
    override def run() = {
      closeTimeoutFiles()
    }
  }, 5, 5, TimeUnit.DAYS)

  def closeAllFiles(): Unit = {
    logger.info("close files")
    for ((fileName, cachedWriterEntity) <- fileCache) {
      try {
        fileCache.remove(fileName)
        syncDFS(cachedWriterEntity)
        if (cachedWriterEntity.needSyncDFS) {
          cachedWriterEntity.dataFileWriter.close()
          cachedWriterEntity.fsDataOutputStream.close()
        }
        logger.info(fileName + " remove from writer cache")
      } catch {
        case e: Exception => logger.error(fileName, e)
      }
    }
  }

}