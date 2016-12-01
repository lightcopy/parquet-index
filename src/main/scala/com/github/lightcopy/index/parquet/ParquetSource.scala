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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, PrimitiveType, Type}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REPEATED, REQUIRED}

import org.apache.spark.sql.Column
import org.apache.spark.sql.types._

import com.github.lightcopy.{Catalog, IndexSpec}
import com.github.lightcopy.index.{Index, IndexSource, Metadata}

class ParquetSource extends IndexSource {
  override def loadIndex(catalog: Catalog, metadata: Metadata): Index = {
    throw new UnsupportedOperationException()
  }

  override def createIndex(catalog: Catalog, spec: IndexSpec, columns: Seq[Column]): Index = {
    throw new UnsupportedOperationException()
  }

  /** Retrieve footer metadata from provided status, expected only one footer */
  private def footerMetadata(conf: Configuration, status: FileStatus): ParquetMetadata = {
    val footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf,
      Arrays.asList(status), false)
    footers.get(0).getParquetMetadata
  }

  /** Convert column into column name, should work for nested types */
  private def withColumnName(column: Column): String = column.toString

  /**
   * Convert Parquet schema into StructType of columns supported by index, it does not convert
   * entire message type. Also note that only certain Parquet types are supported for index.
   * Do not check on MatchError
   */
  private def convert(parquetSchema: MessageType, colNames: Seq[String]): StructType = {
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
  private def convertField(parquetType: Type): DataType = parquetType match {
    case primitive: PrimitiveType => convertPrimitiveField(primitive)
    case group: GroupType => convertGroupField(group)
  }

  /** Convert field into StructField of primitive type */
  private def convertPrimitiveField(tpe: PrimitiveType): DataType = {
    val originalType = tpe.getOriginalType
    val typeName = tpe.getPrimitiveTypeName
    typeName match {
      case BOOLEAN => BooleanType
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
