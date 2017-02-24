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

import scala.util.{Try, Success, Failure}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.parquet.ParquetMetastoreSupport
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/** DataSource to resolve relations that support indexing */
case class IndexedDataSource(
    metastore: Metastore,
    className: String,
    mode: SaveMode = SaveMode.ErrorIfExists,
    options: Map[String, String] = Map.empty,
    bucketSpec: Option[BucketSpec] = None)
  extends Logging {

  lazy val providingClass: Class[_] = IndexedDataSource.lookupDataSource(className)
  lazy val tablePath: FileStatus = {
    val path = options.getOrElse("path", sys.error("path option is required"))
    IndexedDataSource.resolveTablePath(new Path(path),
      metastore.session.sparkContext.hadoopConfiguration)
  }

  def resolveRelation(): BaseRelation = {
    val caseInsensitiveOptions = new CaseInsensitiveMap(options)
    providingClass.newInstance() match {
      case s: MetastoreSupport =>
        // if index does not exist in metastore and option is selected, we will create it before
        // loading index catalog. Note that empty list of columns indicates all available columns
        // will be inferred
        if (metastore.conf.createIfNotExists && !existsIndex()) {
          logInfo("Index does not exist in metastore, will create for all available columns")
          createIndex(Nil)
        }
        logInfo(s"Loading index for $s, table=${tablePath.getPath}")

        val spec = SourceLocationSpec(s.identifier)
        val indexCatalog = metastore.load(spec, tablePath.getPath) { status =>
          s.loadIndex(metastore, status)
        }

        IndexedDataSource.newHadoopRelation(
          metastore.session,
          location = indexCatalog,
          partitionSchema = indexCatalog.partitionSpec().partitionColumns,
          dataSchema = indexCatalog.dataSchema().asNullable,
          bucketSpec = bucketSpec,
          fileFormat = s.fileFormat,
          options = caseInsensitiveOptions)
      case other =>
        throw new UnsupportedOperationException(s"Index is not supported by $other")
    }
  }

  /**
   * Create index based on provided format, if no exception is thrown during creation, considered
   * as process succeeded.
   */
  def createIndex(columns: Seq[Column]): Unit = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      logInfo(s"Create index for $s, table=${tablePath.getPath}, columns=$columns, mode=$mode")
      // infer partitions from file path
      val paths = Seq(tablePath.getPath)
      val partitionSchema: Option[StructType] = None
      val catalog = new ListingFileCatalog(metastore.session, paths, options, partitionSchema,
        ignoreFileNotFound = false)
      val partitionSpec = catalog.partitionSpec
      // ignore filtering expression for partitions, fetch all available files
      val allFiles = catalog.listFiles(Nil)
      val spec = SourceLocationSpec(s.identifier)
      metastore.create(spec, tablePath.getPath, mode) { (status, isAppend) =>
        s.createIndex(metastore, status, tablePath, isAppend, partitionSpec, allFiles, columns)
      }
    case other =>
      throw new UnsupportedOperationException(s"Creation of index is not supported by $other")
  }

  /** Check if index exists, also returns `false` if table path does not exist or corrupt */
  def existsIndex(): Boolean = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      Try {
        logInfo(s"Check index for $s, table=${tablePath.getPath}")
        val spec = SourceLocationSpec(s.identifier)
        metastore.exists(spec, tablePath.getPath)
      } match {
        case Success(exists) =>
          exists
        case Failure(error) =>
          logWarning(s"Error while checking if index exists, $error")
          false
      }
    case other =>
      throw new UnsupportedOperationException(s"Check of index is not supported by $other")
  }

  /** Delete index if exists, otherwise no-op */
  def deleteIndex(): Unit = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      logInfo(s"Delete index for $s, table=${tablePath.getPath}")
      val spec = SourceLocationSpec(s.identifier)
      metastore.delete(spec, tablePath.getPath) { case status =>
        s.deleteIndex(metastore, status) }
    case other =>
      throw new UnsupportedOperationException(s"Deletion of index is not supported by $other")
  }
}

object IndexedDataSource {
  val parquet = classOf[ParquetMetastoreSupport].getCanonicalName
  val hadoopFsRelation = classOf[HadoopFsRelation].getCanonicalName

  /**
   * Resolve class name into fully-qualified class path if available. If no match found, return
   * itself. [[IndexedDataSource]] checks whether or not class is a valid indexed source.
   */
  def resolveClassName(provider: String): String = provider match {
    case "parquet" => parquet
    case "org.apache.spark.sql.execution.datasources.parquet" => parquet
    case other => other
  }

  /** Simplified version of looking up datasource class */
  def lookupDataSource(provider: String): Class[_] = {
    val provider1 = IndexedDataSource.resolveClassName(provider)
    val provider2 = s"$provider.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider1))) match {
      case Success(dataSource) =>
        dataSource
      case Failure(error) =>
        throw new ClassNotFoundException(
          s"Failed to find data source: $provider", error)
    }
  }

  /** Resolve table path into file status, should not contain any glob expansions */
  def resolveTablePath(path: Path, conf: Configuration): FileStatus = {
    val fs = path.getFileSystem(conf)
    fs.getFileStatus(path)
  }

  /**
   * Load `HadoopFsRelation` depending on Spark version.
   * Class in Spark 2.0.0 does not have second list of parameters and SparkSession is passed along
   * with other parameters for HadoopFsRelation. In contrast Spark 2.0.1 and onwards Spark session
   * is passed in second parameter list as part of currying. Here we make it loadable so consumers
   * do not need to worry about compatibility.
   * This is very different from Spark 1.x where HadoopFsRelation resides in `sources` interfaces.
   */
  def newHadoopRelation(
      sparkSession: SparkSession,
      location: FileCatalog,
      partitionSchema: StructType,
      dataSchema: StructType,
      bucketSpec: Option[BucketSpec],
      fileFormat: FileFormat,
      options: Map[String, String]): HadoopFsRelation = {
    // choose constructor depending on Spark version
    val loader = Utils.getContextOrSparkClassLoader
    val clazz = Try(loader.loadClass(hadoopFsRelation)) match {
      case Success(relation) =>
        relation
      case Failure(error) =>
        throw new ClassNotFoundException(s"Failed to find $hadoopFsRelation", error)
    }

    if (sparkSession.version == "2.0.0") {
      val constructor = clazz.getConstructor(
        classOf[SparkSession],
        classOf[FileCatalog],
        classOf[StructType],
        classOf[StructType],
        classOf[Option[BucketSpec]],
        classOf[FileFormat],
        classOf[Map[String, String]])
      constructor.newInstance(sparkSession, location, partitionSchema, dataSchema, bucketSpec,
        fileFormat, options).asInstanceOf[HadoopFsRelation]
    } else {
      // loading constructor for 2.0.1 and onwards including 2.1.x
      val constructor = clazz.getConstructor(
        classOf[FileCatalog],
        classOf[StructType],
        classOf[StructType],
        classOf[Option[BucketSpec]],
        classOf[FileFormat],
        classOf[Map[String, String]],
        classOf[SparkSession])
      constructor.newInstance(location, partitionSchema, dataSchema, bucketSpec, fileFormat,
        options, sparkSession).asInstanceOf[HadoopFsRelation]
    }
  }
}
