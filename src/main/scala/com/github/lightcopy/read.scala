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

package com.github.lightcopy

import org.apache.hadoop.conf.Configuration

import org.apache.parquet.schema._
import org.apache.parquet.io.api._

import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext

class SamplePrimitiveConverter(val index: Int, val schema: PrimitiveType)
  extends PrimitiveConverter {

  override def addBinary(value: Binary): Unit = { }

  override def addBoolean(value: Boolean): Unit = { }

  override def addDouble(value: Double): Unit = { }

  override def addInt(value: Int): Unit = { }

  override def addLong(value: Long): Unit = { }
}

class SampleGroupConverter(val index: Int, val schema: GroupType) extends GroupConverter {
  private val converters = prepareConverters(schema)
  private val offset = " " * index

  private def prepareConverters(schema: GroupType): Array[Converter] = {
    println(s"${offset}+ Read schema $schema")
    val arr = new Array[Converter](schema.getFieldCount)
    for (i <- 0 until arr.length) {
      val tpe = schema.getType(i)
      if (tpe.isPrimitive) {
        arr(i) = new SamplePrimitiveConverter(i, tpe.asPrimitiveType)
      } else {
        arr(i) = new SampleGroupConverter(i, tpe.asGroupType)
      }
    }
    arr
  }

  def getCurrentRecord(): String = "test"

  override def getConverter(fieldIndex: Int): Converter = {
    val conv = converters(fieldIndex)
    println(s"${offset}+ Use converter at $fieldIndex index = $conv")
    conv
  }

  override def start(): Unit = {
    println(s"${offset}+ Start parsing group record, index=$index, schema=$schema")
  }

  override def end(): Unit = {
    println(s"${offset}+ End parsing group record")
  }
}

class SampleRecordMaterializer(val schema: MessageType) extends RecordMaterializer[String] {
  private var record: String = _
  private val root = new SampleGroupConverter(0, schema)

  override def getCurrentRecord(): String = root.getCurrentRecord

  override def getRootConverter(): GroupConverter = root
}

class SampleReadSupport extends ReadSupport[String] {
  override def init(
      conf: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType): ReadContext = {
    val partialSchemaString = conf.get(ReadSupport.PARQUET_READ_SCHEMA)
    val requestedProjection = ReadSupport.getSchemaForRead(fileSchema, partialSchemaString)
    new ReadContext(requestedProjection)
  }

  override def prepareForRead(
      conf: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType, context: ReadContext): RecordMaterializer[String] = {
    new SampleRecordMaterializer(context.getRequestedSchema)
  }
}
