/*
 * Copyright 2016 Lightcopy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException
import java.util.Arrays

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.{DataType, StructType}

import com.github.lightcopy.util.{SerializableFileStatus, IOUtils}

case class ParquetMetastoreSupport() extends MetastoreSupport with Logging {
  override def identifier: String = "parquet"

  override def fileFormat: FileFormat = new ParquetFileFormat()

  override def loadIndex(
      metastore: Metastore,
      indexDirectory: FileStatus): MetastoreIndexCatalog = {
    val readDir = tableMetadataLocation(indexDirectory.getPath)
    if (!metastore.fs.exists(readDir)) {
      throw new IOException(s"Path $readDir for table metadata does not exist")
    }

    val metadataDir = tableMetadataLocation(indexDirectory.getPath)
    var indexMetadata: ParquetIndexMetadata = null
    IOUtils.readContentStream(metastore.fs, metadataDir) { in =>
      val kryo = new KryoSerializer(metastore.session.sparkContext.getConf)
      val deserializedStream = kryo.newInstance.deserializeStream(in)
      try {
        indexMetadata = deserializedStream.readObject()
      } catch {
        case NonFatal(err) =>
          throw new IOException("Failed to deserialize object", err)
      } finally {
        deserializedStream.close()
      }
    }

    // if eager loading is enabled, load all filter statistics
    // this operation is done once, catalog will be cached in metastore
    if (metastore.conf.parquetFilterEagerLoading && indexMetadata != null) {
      logInfo("Loading all filter statistics for catalog")
      val startTime = System.nanoTime()
      implicit val executorContext = ExecutionContext.global
      val futures: Seq[Future[Unit]] = indexMetadata.partitions.flatMap { partition =>
        partition.files.flatMap { status =>
          status.blocks.flatMap { block =>
            block.indexedColumns.values.flatMap { metadata =>
              metadata.filter match {
                case Some(filter) => Some(Future[Unit] {
                  filter.readData(metastore.fs) }(executorContext))
                case None => None
              }
            }
          }
        }
      }
      Await.ready(Future.sequence(futures), Duration.Inf)
      val endTime = System.nanoTime()
      def timeMs: Double = (endTime - startTime).toDouble / 1000000
      logInfo(s"Loaded all filter statistics for catalog in $timeMs ms")
    }

    new ParquetIndexCatalog(metastore, indexMetadata)
  }

  override def createIndex(
      metastore: Metastore,
      indexDirectory: FileStatus,
      tablePath: FileStatus,
      isAppend: Boolean,
      partitionSpec: PartitionSpec,
      partitions: Seq[Partition],
      columns: Seq[Column]): Unit = {
    require(partitions.nonEmpty, "Empty partitions provided")
    if (isAppend) {
      throw new UnsupportedOperationException(
        s"${getClass.getSimpleName} does not support append to existing index")
    }

    val columnNames = columns.map(_.toString)
    // make sure that columns are not part of partition columns
    val fieldNames = partitionSpec.partitionColumns.fieldNames.toSet
    for (name <- columnNames) {
      if (fieldNames.contains(name)) {
        throw new IllegalArgumentException(s"Found column $name in partitioning schema. " +
          "Currently indexing of partitioning columns is not supported")
      }
    }

    val partitionSchema = partitionSpec.partitionColumns
    // prepare index schema by fetching message type from first partition
    val indexSchema = inferIndexSchema(metastore, partitions, columnNames)
    logInfo(s"Resolved index schema ${indexSchema.simpleString}")

    // serialized file statuses for Parquet table
    val files = partitions.flatMap { partition =>
      partition.files.map { status =>
        SerializableFileStatus.fromFileStatus(status)
      }
    }

    val sc = metastore.session.sparkContext
    val hadoopConf = metastore.session.sessionState.newHadoopConf()
    if (metastore.conf.parquetFilterEnabled) {
      hadoopConf.set(ParquetMetastoreSupport.FILTER_DIR, indexDirectory.getPath.toString)
      hadoopConf.set(ParquetMetastoreSupport.FILTER_TYPE, metastore.conf.parquetFilterType)
    }

    val numPartitions = ParquetMetastoreSupport.inferNumPartitions(metastore)
    val rdd = new ParquetStatisticsRDD(sc, hadoopConf, indexSchema, files, numPartitions)
    val statistics = rdd.collect

    val extendedPartitions = partitions.map { partition =>
      ParquetPartition(partition.values, partition.files.map { status =>
        // search status filepath in collected statistics and replace
        // assume that there is only one path per partition
        val maybeStats = statistics.find { stats => stats.status.path == status.getPath.toString }
        assert(maybeStats.nonEmpty, "No match found when converting statistics to partitions, " +
          s"failed status = $status")
        maybeStats.get
      })
    }

    // full index metadata, data schema does not include partitioning columns, this will be added
    // by data source when reading format
    val indexMetadata = ParquetIndexMetadata(
      tablePath = tablePath.getPath.toString,
      dataSchema = ParquetMetastoreSupport.inferSchema(metastore.session, statistics),
      indexSchema = indexSchema,
      partitionSpec = partitionSpec,
      partitions = extendedPartitions)

    // write table to disk, create path with table metadata
    val metadataDir = tableMetadataLocation(indexDirectory.getPath)
    IOUtils.writeContentStream(metastore.fs, metadataDir) { out =>
      val kryo = new KryoSerializer(sc.getConf)
      val serializedStream = kryo.newInstance().serializeStream(out)
      try {
        serializedStream.writeObject(indexMetadata)
      } catch {
        case NonFatal(err) =>
          throw new IOException("Failed to serialize object", err)
      } finally {
        serializedStream.close()
      }
    }
  }

  /**
   * Infer schema by requesting provided set of columns from a single partition.
   * If list of column names is empty, infer all available columns in schema.
   */
  private[parquet] def inferIndexSchema(
      metastore: Metastore, partitions: Seq[Partition], columns: Seq[String]): StructType = {
    val conf = metastore.session.sessionState.newHadoopConf()
    val status = partitions.head.files.head

    // read footer and extract schema into struct type
    val footer = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf,
      Arrays.asList(status), false).get(0)
    val schema = footer.getParquetMetadata.getFileMetaData.getSchema
    val fileStruct = new ParquetSchemaConverter().convert(schema)

    // if no columns provided prune struct type and return only valid columns, otherwise do
    // normal column check
    if (columns.isEmpty) {
      ParquetSchemaUtils.pruneStructType(fileStruct)
    } else {
      // fetch specified columns, inferred fields should be the same as requested columns
      val inferredSchema = StructType(fileStruct.filter { field => columns.contains(field.name) })
      columns.foreach { name =>
        val containsField = inferredSchema.exists { _.name == name }
        if (!containsField) {
          throw new IllegalArgumentException("Failed to select indexed columns. " +
            s"Column $name does not exist in inferred schema ${inferredSchema.simpleString}")
        }
      }

      inferredSchema
    }
  }

  private def tableMetadataLocation(root: Path): Path = {
    root.suffix(s"${Path.SEPARATOR}${ParquetMetastoreSupport.TABLE_METADATA}")
  }
}

object ParquetMetastoreSupport extends Logging {
  // internal Hadoop configuration option to set schema for Parquet reader
  private[sql] val READ_SCHEMA = "spark.sql.hadoop.index.parquet.read.schema"
  // internal Hadoop configuration option to specify filter directory
  private[sql] val FILTER_DIR = "spark.sql.hadoop.index.parquet.filter.dir"
  // internal Hadoop configuration option to specify filter name/type
  private[sql] val FILTER_TYPE = "spark.sql.hadoop.index.parquet.filter.type"
  // internal footer metadata key for Spark SQL schema
  private[sql] val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"
  // metadata name
  val TABLE_METADATA = "_table_metadata"

  /**
   * Infer schema from collected statistics.
   * Partition columns are automatically added when reading file format
   */
  def inferSchema(sparkSession: SparkSession, statistics: Array[ParquetFileStatus]): StructType = {
    // create converter to fall back to Parquet schema parsing
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    val writeLegacyParquetFormat = sparkSession.sessionState.conf.writeLegacyParquetFormat
    val converter = new ParquetSchemaConverter(
      assumeBinaryIsString = assumeBinaryIsString,
      assumeInt96IsTimestamp = assumeInt96IsTimestamp,
      writeLegacyParquetFormat = writeLegacyParquetFormat)

    // util to convert Parquet schema into Spark SQL schema using global converter
    def parquetToStructType(parquetSchema: String): StructType = {
      converter.convert(MessageTypeParser.parseMessageType(parquetSchema))
    }

    // Parse Spark SQL schema, if found, otherwise fall back to the Parquet schema
    val schemas: Array[StructType] = statistics.map { status =>
      val parquetSchema = status.fileSchema
      status.sqlSchema match {
        case Some(serializedSchema) =>
          try {
            DataType.fromJson(serializedSchema).asInstanceOf[StructType]
          } catch {
            case NonFatal(err) =>
              logWarning(s"Failed to parse Spark SQL serialized schema $serializedSchema, " +
                s"reason: $err. Using Parquet file schema")
              parquetToStructType(status.fileSchema)
          }
        case None =>
          parquetToStructType(status.fileSchema)
      }
    }

    // Merge into final schema
    schemas.reduceOption { (left, right) =>
      ParquetSchemaUtils.merge(left, right)
    }.getOrElse(StructType(Nil))
  }

  /**
   * Infer number of partitions for `sc.parallelize`. When configuration is provided and it is a
   * positive value, return it, otherwise compute default number of partitions to use. This is
   * selected as min value between sc.defaultParallelism * 3 and shuffle partitions setting.
   */
  def inferNumPartitions(metastore: Metastore): Int = {
    if (metastore.conf.numPartitions > 0) {
      metastore.conf.numPartitions
    } else {
      val defaultParallelism = metastore.session.sparkContext.defaultParallelism * 3
      val shufflePartitions = metastore.session.conf.get("spark.sql.shuffle.partitions").toInt
      Math.min(defaultParallelism, shufflePartitions)
    }
  }
}
