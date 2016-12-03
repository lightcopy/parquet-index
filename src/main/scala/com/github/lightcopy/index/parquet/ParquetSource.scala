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

package com.github.lightcopy.index.parquet

import java.util.Arrays

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}

import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, PrimitiveType, Type}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REPEATED, REQUIRED}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import com.github.lightcopy.{Catalog, FileSystemCatalog, IndexSpec}
import com.github.lightcopy.index.{Index, FileSource, Metadata}
import com.github.lightcopy.util.IOUtils

class ParquetIndex(
    val ctg: Catalog,
    val metadata: Metadata,
    val indexDir: FileStatus,
    val paths: Array[FileStatus])
  extends Index {

  require(metadata.path.isDefined, "Parquet index requires metadata path")
  @transient private val sc = ctg.asInstanceOf[FileSystemCatalog].sqlContext.sparkContext
  private val index = load()
  IOUtils.writeContent(catalog.fs,
    indexDir.getPath.suffix(s"${Path.SEPARATOR}data"), serialize(index))

  override def getIndexIdentifier(): String = indexDir.getPath.toString

  override def getMetadata(): Metadata = metadata

  override def catalog: Catalog = ctg

  override def containsSpec(spec: IndexSpec): Boolean = {
    spec.path match {
      case Some(file) =>
        try {
          val status = catalog.fs.getFileStatus(new Path(file))
          status.getPath.toString == metadata.path.get
        } catch {
          case NonFatal(err) => false
        }
      case None => false
    }
  }

  override def append(spec: IndexSpec, columns: Seq[Column]): Unit = {
    throw new UnsupportedOperationException()
  }

  override def search(condition: Column): DataFrame = {
    throw new UnsupportedOperationException()
  }

  override def delete(): Unit = {
    throw new UnsupportedOperationException()
  }

  private def load(): Seq[ParquetFileStatistics] = {
    val data = paths.map { status =>
      ParquetFileStatus(status.getPath.toString, status.getLen) }.toSeq
    val rdd = new StatisticsRDD(sc, catalog.fs.getConf, metadata.columns, data,
      sc.defaultParallelism * 3)
    rdd.collect.toSeq
  }

  private def serialize(index: Seq[ParquetFileStatistics]): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val buffer = new ArrayBuffer[String]()
    for (part <- index) {
      buffer.append(Serialization.write[ParquetFileStatistics](part))
    }
    buffer.mkString("\n")
  }
}

/**
 * [[ParquetSource]] provides mechanism to create and load index for Parquet table, which can reside
 * on HDFS or local file-system. Index is file-system based, meaning metadata and index information
 * are stored on disk, catalog might provide ability to cache index in memory.
 *
 * Source supports only limited set of fields that can be indexed, e.g. numeric fields, and string
 * columns. When resolving columns, only one footer is used extract schema and convert it into
 * StructType, so it is not recommended to use index across files with different but mergeable
 * schema.
 */
class ParquetSource extends FileSource {
  override def loadFileIndex(catalog: Catalog, metadata: Metadata): Index = {
    throw new UnsupportedOperationException()
  }

  override def createFileIndex(
      catalog: Catalog,
      indexDir: FileStatus,
      tablePath: FileStatus,
      paths: Array[FileStatus],
      colNames: Seq[String]): Index = {
    val status = paths.head
    val parquetMetadata = footerMetadata(catalog.fs.getConf, status)
    val parquetSchema = parquetMetadata.getFileMetaData.getSchema
    val schema = ParquetSource.convert(parquetSchema, colNames)
    val metadata = Metadata(ParquetSource.PARQUET_SOURCE_FORMAT, Some(tablePath.getPath.toString),
      schema, Map.empty)
    new ParquetIndex(catalog, metadata, indexDir, paths)
  }

  /** Parquet source discards global metadata files and commit files, e.g. _SUCCESS */
  override def pathFilter(): PathFilter = new PathFilter() {
    override def accept(path: Path): Boolean = {
      path.getName != ParquetFileWriter.PARQUET_METADATA_FILE &&
      path.getName != ParquetFileWriter.PARQUET_COMMON_METADATA_FILE &&
      path.getName != ParquetSource.SUCCESS_FILE
    }
  }

  /** Retrieve footer metadata from provided status, expected only one footer */
  private def footerMetadata(conf: Configuration, status: FileStatus): ParquetMetadata = {
    val footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf,
      Arrays.asList(status), false)
    footers.get(0).getParquetMetadata
  }
}

/** Contains constants and schema converter */
private[lightcopy] object ParquetSource {
  // Format to reference Parquet datasource
  val PARQUET_SOURCE_FORMAT = "parquet"
  // Commit file name to discard
  val SUCCESS_FILE = "_SUCCESS"

  /**
   * Convert Parquet schema into StructType of columns supported by index, it does not convert
   * entire message type. Also note that only certain Parquet types are supported for index.
   * Do not check on MatchError
   */
  def convert(parquetSchema: MessageType, colNames: Seq[String]): StructType = {
    val fieldsSet = colNames.toSet
    val fields = parquetSchema.getFields.asScala.flatMap { field =>
      if (fieldsSet.contains(field.getName)) {
        val structField = field.getRepetition match {
          case OPTIONAL => StructField(field.getName, convertField(field), nullable = true)
          case REQUIRED => StructField(field.getName, convertField(field), nullable = false)
          case REPEATED => throw new UnsupportedOperationException(
            "Parquet repeated fields are not supported")
        }
        Some(structField)
      } else {
        None
      }
    }

    if (fields.length != colNames.length) {
      throw new IllegalStateException(
        s"Schema mismatch, requested columns ${colNames.mkString("[", ", ", "]")} are different " +
        s"from extracted fields ${fields.mkString("[", ", ", "]")} for schema $parquetSchema")
    }

    StructType(fields)
  }

  /** Convert individual field into `StructField`, do not check on MatchError */
  def convertField(parquetType: Type): DataType = parquetType match {
    case primitive: PrimitiveType => convertPrimitiveField(primitive)
    case group: GroupType => convertGroupField(group)
  }

  /** Convert field into StructField of primitive type */
  def convertPrimitiveField(tpe: PrimitiveType): DataType = {
    val originalType = tpe.getOriginalType
    val typeName = tpe.getPrimitiveTypeName
    typeName match {
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case INT32 =>
        originalType match {
          case INT_8 => ByteType
          case INT_16 => ShortType
          case INT_32 | null => IntegerType
          case other => typeNotSupported(other)
        }
      case INT64 =>
        originalType match {
          case INT_64 | null => LongType
          case other => typeNotSupported(other)
        }
      case BINARY =>
        originalType match {
          case UTF8 | ENUM | JSON => StringType
          case other => typeNotSupported(other)
        }
      case otherTypeName =>
        throw new UnsupportedOperationException(
          s"Parquet primitive type $otherTypeName is not supported")
    }
  }

  /** Convert field into StructField of group type */
  private def convertGroupField(field: GroupType): DataType = {
    throw new UnsupportedOperationException(s"Parquet index does not support group type $field")
  }

  /** Type not support error */
  private def typeNotSupported(tpe: OriginalType): DataType = {
    throw new UnsupportedOperationException(s"Parquet original type $tpe is not supported")
  }
}
