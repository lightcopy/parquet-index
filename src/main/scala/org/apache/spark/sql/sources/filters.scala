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

package org.apache.spark.sql.sources

/**
 * Trivial filter to resolve directly.
 * It is either AlwaysTrue or AlwaysFalse depending on the value.
 */
object Trivial {
  def apply(value: Boolean): Filter = {
    if (value) AlwaysTrue else AlwaysFalse
  }

  def unapply(filter: Filter): Option[Boolean] = filter match {
    case AlwaysTrue => Some(true)
    case AlwaysFalse => Some(false)
    case _ => None
  }
}
