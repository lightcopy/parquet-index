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

package com.github.lightcopy.util

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

/**
 * Serializable version of `BlocLocation` of HDFS.
 */
case class SerializableBlockLocation(
  names: Array[String],
  hosts: Array[String],
  offset: Long,
  length: Long)

/**
 * Serializable version of `FileStatus`of HDFS.
 */
case class SerializableFileStatus(
  path: String,
  length: Long,
  isDir: Boolean,
  blockReplication: Short,
  blockSize: Long,
  modificationTime: Long,
  accessTime: Long,
  blockLocations: Array[SerializableBlockLocation])

/**
 * Util to convert serializable file status into HDFS `LocatedFileStatus`. Currently file status is
 * assumed to have empty block locations, if it was serialized from non-`LocatedFileStatus`.
 */
object SerializableFileStatus {
  def fromFileStatus(status: FileStatus): SerializableFileStatus = {
    val blockLocations = status match {
      case f: LocatedFileStatus =>
        f.getBlockLocations.map { loc =>
          SerializableBlockLocation(loc.getNames, loc.getHosts, loc.getOffset, loc.getLength)
        }
      case _ =>
        Array.empty[SerializableBlockLocation]
    }

    SerializableFileStatus(
      status.getPath.toString,
      status.getLen,
      status.isDirectory,
      status.getReplication,
      status.getBlockSize,
      status.getModificationTime,
      status.getAccessTime,
      blockLocations)
  }

  def toFileStatus(status: SerializableFileStatus): FileStatus = {
    val blockLocations = status.blockLocations.map { loc =>
      new BlockLocation(loc.names, loc.hosts, loc.offset, loc.length)
    }

    new LocatedFileStatus(
      new FileStatus(
        status.length,
        status.isDir,
        status.blockReplication,
        status.blockSize,
        status.modificationTime,
        new Path(status.path)),
      blockLocations)
  }
}
