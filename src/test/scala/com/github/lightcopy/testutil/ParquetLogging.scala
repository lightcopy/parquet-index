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

package com.github.lightcopy.testutil

import java.util.logging._

/**
 * Workaround to disable Parquet logging (org.apache.parquet), which is very verbose and is not
 * particularly meaningful unless debugging Parquet read/write functionality. Currently test trait
 * is inherited in UnitTestSuite.
 */
trait ParquetLogging {
  val logger = Logger.getLogger("org.apache.parquet")
  val handlers: Array[Handler] = logger.getHandlers()
  if (handlers == null || handlers.length == 0) {
    println("[LOGGING] Found no handlers for org.apache.parquet, add no-op logging")
    val handler = new ConsoleHandler()
    handler.setLevel(Level.OFF)
    logger.addHandler(handler)
    logger.setLevel(Level.OFF)
  }
}
