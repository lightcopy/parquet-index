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

package org.apache.spark.sql.execution.datasources.lightweight

import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.DataType

case class PartitionIndexFilters(pos: Int, dataType: DataType, fileSize: Int) {

  /**
    * Recursively fold provided index filters to trivial,
    * blocks are always non-empty.
    */
  def foldFilter(filter: Filter): Filter = {
    filter match {
      case EqualTo(attribute: String, value: Any) =>
        if (pos == hash(value)) {
          Trivial(true)
        } else {
          Trivial(false)
        }
      case In(attribute: String, values: Array[Any]) =>
        val r = values.collectFirst { case v if pos == hash(v) => v }
        if (r.isDefined) {
          Trivial(true)
        } else {
          Trivial(false)
        }
      case And(left: Filter, right: Filter) =>
        And(foldFilter(left), foldFilter(right)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldFilter(left), foldFilter(right)) match {
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case other => other
        }
      case Not(child: Filter) =>
        Not(foldFilter(child)) match {
          case Not(Trivial(false)) => Trivial(true)
          case Not(Trivial(true)) => Trivial(false)
          case other => other
        }
      case trivial: Trivial =>
        trivial
      case unsupportedFilter =>
        // return 'true' to scan all partitions
        // currently unsupported filters are:
        // - IsNotNull
        // - StringStartsWith
        // - StringEndsWith
        // - StringContains
        // - EqualNullSafe
        Trivial(true)
    }
  }

  // copied from Spark:Pmod(new Murmur3Hash(expressions), Literal(numPartitions))
  def hash(value: Any): Int = {
    val len = 13
    pmod(Murmur3HashFunction.hash(value, dataType, 42), len.asInstanceOf[Long]).toInt
  }

  // copied from org.apache.spark.sql.catalyst.expressions.Pmod#pmod
  private def pmod(a: Long, n: Long): Long = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }
}
