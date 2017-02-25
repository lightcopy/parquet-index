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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

/**
 * [[IndexIdentifier]] describes index location in metastore by providing information about
 * metastore support identifier and dataspace for index, e.g. datasource or catalog table.
 */
abstract class IndexLocationSpec private[datasources] {

  /** Raw value for identifier, will be resolved on the first access */
  protected def unresolvedIndentifier: String

  /** Raw value for dataspace, will be resolved on the first access */
  protected def unresolvedDataspace: String

  /** Path to the source table that is backed by filesystem-based datasource */
  def sourcePath: Path

  /** Support string identifier */
  lazy val identifier: String = {
    validate(unresolvedIndentifier, "identifier")
    unresolvedIndentifier
  }

  /** Dataspace identifier, must be one of the supported spaces */
  lazy val dataspace: String = {
    validate(unresolvedDataspace, "dataspace")
    unresolvedDataspace
  }

  /** Validate property of the location spec */
  private def validate(value: String, property: String): Unit = {
    require(value != null && value.nonEmpty, s"Empty $property")
    value.foreach { ch =>
      require(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z', s"Invalid character $ch in " +
        s"$property $value. Only lowercase alpha-numeric characters are supported")
    }
  }

  override def toString(): String = {
    s"[$dataspace/$identifier, source=$sourcePath]"
  }
}

/** Location spec for datasource table */
private[datasources] case class SourceLocationSpec(
    unresolvedIndentifier: String,
    sourcePath: Path)
  extends IndexLocationSpec {

  override protected def unresolvedDataspace: String = "source"
}

/** Location spec for catalog (persisten) table */
private[datasources] case class CatalogLocationSpec(
    unresolvedIndentifier: String,
    sourcePath: Path)
  extends IndexLocationSpec {

  override protected def unresolvedDataspace: String = "catalog"
}
