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

package com.github.lightcopy.index

import org.json4s._
import org.json4s.jackson.{JsonMethods => JSON, Serialization}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Index metadata, maps directly to JSON.
 * @param source source to use when loading index, e.g. "parquet"
 * @param path optional path to the datasource table that is used for index
 * @param columns set of columns that are supported by index
 * @param options specific index options, depends on implementation
 */
case class Metadata(
    source: String,
    path: Option[String],
    columns: StructType,
    options: Map[String, String]) {

  override def toString(): String = {
    s"${getClass.getSimpleName} ${SerDe.serialize(this)}"
  }
}

/** Custom StructType serializer to use for case class extraction */
private class StructTypeSerializer extends Serializer[StructType] {
  val StructTypeClass = classOf[StructType]

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), StructType] = {
    case (TypeInfo(StructTypeClass, _), json) => json match {
      case struct @ JObject(JField("type", JString("struct")) :: fields) =>
        DataType.fromJson(JSON.compact(struct)).asInstanceOf[StructType]
      case other => throw new MappingException(s"Can't convert $other to $StructTypeClass")
    }
  }

  def serialize(implicit formats: Formats): PartialFunction[Any, JValue] = {
    case struct: StructType => JSON.parse(struct.json)
  }
}

/** SerDe for [[Metadata]], provides some basic serialization/deserialization of case class */
private[index] object SerDe {
  implicit val formats = Serialization.formats(NoTypeHints) + new StructTypeSerializer()

  /** Convert metadata into JSON string */
  def serialize(metadata: Metadata): String = {
    Serialization.write[Metadata](metadata)
  }

  /** Convert JSON string into Metadata */
  def deserialize(json: String): Metadata = {
    Serialization.read[Metadata](json)
  }
}
