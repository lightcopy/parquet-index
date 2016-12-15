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

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.hadoop.conf.Configuration

import org.apache.parquet.schema._
import org.apache.parquet.io.api._

import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext

abstract class Container {
  def setString(ordinal: Int, value: String): Unit
  def setBoolean(ordinal: Int, value: Boolean): Unit
  def setDouble(ordinal: Int, value: Double): Unit
  def setInt(ordinal: Int, value: Int): Unit
  def setLong(ordinal: Int, value: Long): Unit
  def getByIndex(ordinal: Int): Any
  def init(): Unit
  def close(): Unit
}

private[parquet] class MapContainer extends Container {
  // buffer to store values in container, column index - value
  private var buffer: JHashMap[Int, Any] = null

  override def setString(ordinal: Int, value: String): Unit = buffer.put(ordinal, value)
  override def setBoolean(ordinal: Int, value: Boolean): Unit = buffer.put(ordinal, value)
  override def setDouble(ordinal: Int, value: Double): Unit = buffer.put(ordinal, value)
  override def setInt(ordinal: Int, value: Int): Unit = buffer.put(ordinal, value)
  override def setLong(ordinal: Int, value: Long): Unit = buffer.put(ordinal, value)
  override def getByIndex(ordinal: Int): Any = buffer.get(ordinal)

  override def init(): Unit = {
    if (buffer == null) {
      buffer = new JHashMap[Int, Any]()
    } else {
      buffer.clear()
    }
  }

  override def close(): Unit = { }

  override def toString(): String = {
    s"${getClass.getSimpleName}(buffer=$buffer)"
  }
}

class ParquetIndexPrimitiveConverter(
    val ordinal: Int,
    val updater: Container)
  extends PrimitiveConverter {

  override def addBinary(value: Binary): Unit = updater.setString(ordinal, value.toStringUsingUTF8)
  override def addBoolean(value: Boolean): Unit = updater.setBoolean(ordinal, value)
  override def addDouble(value: Double): Unit = updater.setDouble(ordinal, value)
  override def addInt(value: Int): Unit = updater.setInt(ordinal, value)
  override def addLong(value: Long): Unit = updater.setLong(ordinal, value)
}

class ParquetIndexGroupConverter(
    private val updater: Container,
    private val schema: GroupType)
  extends GroupConverter {

  private val converters = prepareConverters(schema)

  private def prepareConverters(schema: GroupType): Array[Converter] = {
    val arr = new Array[Converter](schema.getFieldCount)
    for (i <- 0 until arr.length) {
      val tpe = schema.getType(i)
      assert(tpe.isPrimitive, s"Only primitive types are supported, found schema $schema")
      arr(i) = new ParquetIndexPrimitiveConverter(i, updater)
    }
    arr
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  override def start(): Unit = updater.init()

  override def end(): Unit = updater.close()
}

class ParquetIndexRecordMaterializer(
    private val updater: Container,
    private val schema: MessageType)
  extends RecordMaterializer[Container] {

  override def getCurrentRecord(): Container = updater

  override def getRootConverter(): GroupConverter = {
    new ParquetIndexGroupConverter(updater, schema)
  }
}

class ParquetIndexSupport extends ReadSupport[Container] {
  override def init(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType): ReadContext = {
    val partialSchemaString = conf.get(ParquetIndexFileFormat.READ_SCHEMA)
    val requestedProjection = ReadSupport.getSchemaForRead(fileSchema, partialSchemaString)
    new ReadContext(requestedProjection)
  }

  override def prepareForRead(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType, context: ReadContext): RecordMaterializer[Container] = {
    val updater = new MapContainer()
    new ParquetIndexRecordMaterializer(updater, context.getRequestedSchema)
  }
}
