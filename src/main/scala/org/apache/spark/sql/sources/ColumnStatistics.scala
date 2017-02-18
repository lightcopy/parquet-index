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

import org.apache.spark.sql.types._

/**
 * [[ColumnStatistics]] class provides generic implementation for null-tolerant statistics, and
 * holds information about nulls already. Subclasses need to implement partial functions to handle
 * their data types only. Note that statistics should be serializable.
 */
abstract class ColumnStatistics extends Serializable {
  // number of nulls collected for statistics
  private var numNulls: Long = 0

  /**
   * Partial function for updating statistics.
   * Should provide implementation for certain statistics type only.
   */
  protected def updateMinMaxFunc: PartialFunction[Any, Unit]

  /**
   * Partial function for checking if value is within statistics.
   * Should provide implementation for certain statistics type only.
   */
  protected def containsFunc: PartialFunction[Any, Boolean]

  /**
   * Partial function for checking if value is less than statistics min.
   * Should provide implementation for certain statistics type only.
   */
  protected def isLessThanMinFunc: PartialFunction[Any, Boolean]

  /**
   * Partial function for checking if value is greater than statistics max.
   * Should provide implementation for certain statistics type only.
   */
  protected def isGreaterThanMaxFunc: PartialFunction[Any, Boolean]

  /**
   * Partial function for checking if value is equal to statistics min.
   * Should provide implementation for certain statistics type only.
   */
  protected def isEqualToMinFunc: PartialFunction[Any, Boolean]

  /**
   * Partial function for checking if value is equal to statistics max.
   * Should provide implementation for certain statistics type only.
   */
  protected def isEqualToMaxFunc: PartialFunction[Any, Boolean]

  /** Get minimal value of column, mainly for testing */
  def getMin(): Any

  /** Get maximum value of column, mainly for testing */
  def getMax(): Any

  /** Get number of nulls in column */
  def getNumNulls(): Long = numNulls

  /** Whether or not this column has null values */
  def hasNull(): Boolean = getNumNulls > 0

  /** Increment number of nulls */
  def incrementNumNulls(): Unit = numNulls += 1

  /**
   * Update statistics with non-null value, min and max should be updated according to column type,
   * e.g. sorting of bytes for strings, etc. It is guaranteed that method will be called for
   * defined value. If type does not match, exception is thrown.
   */
  def updateMinMax(value: Any): Unit = {
    updateMinMaxFunc.applyOrElse[Any, Unit](value, { case other =>
      throw new UnsupportedOperationException(s"$this does not support value $other for update")
    })
  }

  /**
   * Return true, if provided value is of compatible type and within range between min and max,
   * or null if statistics contain any nulls. See `containsFunc` for specific typed implementation.
   * Method is null-tolerant, so nulls are checked against number of nulls for statistics.
   */
  def contains(value: Any): Boolean = {
    containsFunc.applyOrElse[Any, Boolean](value, {
      case other if other == null && hasNull => true
      case other => false
    })
  }

  /**
   * Return true if value is less than min.
   * Method is null-intolerant and returns false for null values or values of wrong types.
   */
  def isLessThanMin(value: Any): Boolean = {
    isLessThanMinFunc.applyOrElse[Any, Boolean](value, {
      case other => false
    })
  }

  /**
   * Return true if value is greater than max.
   * Method is null-intolerant and returns false for null values or values of wrong types.
   */
  def isGreaterThanMax(value: Any): Boolean = {
    isGreaterThanMaxFunc.applyOrElse[Any, Boolean](value, {
      case other => false
    })
  }

  /**
   * Return true if value is equal to min.
   * Method is null-intolerant and returns false for null values or values of wrong types.
   */
  def isEqualToMin(value: Any): Boolean = {
    isEqualToMinFunc.applyOrElse[Any, Boolean](value, {
      case other => false
    })
  }

  /**
   * Return true if value is equal to max.
   * Method is null-intolerant and returns false for null values or values of wrong types.
   */
  def isEqualToMax(value: Any): Boolean = {
    isEqualToMaxFunc.applyOrElse[Any, Boolean](value, {
      case other => false
    })
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}[min=${getMin}, max=${getMax}, nulls=${getNumNulls}]"
  }
}

object ColumnStatistics {
  /** Get statistics for Spark SQL data type */
  def getStatisticsForType(tpe: DataType): ColumnStatistics = tpe match {
    case IntegerType => IntColumnStatistics()
    case LongType => LongColumnStatistics()
    case StringType => StringColumnStatistics()
    case DateType => DateColumnStatistics()
    case TimestampType => TimestampColumnStatistics()
    case other => throw new UnsupportedOperationException(
      s"Column statistics do not exist for type $other")
  }
}

/**
 * [[IntColumnStatistics]] keep track of min/max/nulls for signed INT32 column.
 */
case class IntColumnStatistics() extends ColumnStatistics {
  private var min: Int = Int.MaxValue
  private var max: Int = Int.MinValue
  private var isSet: Boolean = false

  override protected def updateMinMaxFunc: PartialFunction[Any, Unit] = {
    case intValue: Int => {
      if (!isSet) {
        min = intValue
        max = intValue
        isSet = true
      } else {
        if (min > intValue) min = intValue
        if (max < intValue) max = intValue
      }
    }
  }

  override protected def containsFunc: PartialFunction[Any, Boolean] = {
    case intValue: Int if isSet => intValue >= min && intValue <= max
  }

  override protected def isLessThanMinFunc: PartialFunction[Any, Boolean] = {
    case intValue: Int if isSet => intValue < min
  }

  override protected def isGreaterThanMaxFunc: PartialFunction[Any, Boolean] = {
    case intValue: Int if isSet => intValue > max
  }

  override protected def isEqualToMinFunc: PartialFunction[Any, Boolean] = {
    case intValue: Int if isSet => intValue == min
  }

  override protected def isEqualToMaxFunc: PartialFunction[Any, Boolean] = {
    case intValue: Int if isSet => intValue == max
  }

  override def getMin(): Any = if (isSet) min else null

  override def getMax(): Any = if (isSet) max else null
}

/**
 * [[LongColumnStatistics]] keep track of min/max/nulls for signed INT64 column.
 * Timestamp columns are not supported by this statistics.
 */
case class LongColumnStatistics() extends ColumnStatistics {
  private var min: Long = Long.MaxValue
  private var max: Long = Long.MinValue
  private var isSet: Boolean = false

  override protected def updateMinMaxFunc: PartialFunction[Any, Unit] = {
    case longValue: Long => {
      if (!isSet) {
        min = longValue
        max = longValue
        isSet = true
      } else {
        if (min > longValue) min = longValue
        if (max < longValue) max = longValue
      }
    }
  }

  override protected def containsFunc: PartialFunction[Any, Boolean] = {
    case longValue: Long if isSet => longValue >= min && longValue <= max
  }

  override protected def isLessThanMinFunc: PartialFunction[Any, Boolean] = {
    case longValue: Long if isSet => longValue < min
  }

  override protected def isGreaterThanMaxFunc: PartialFunction[Any, Boolean] = {
    case longValue: Long if isSet => longValue > max
  }

  override protected def isEqualToMinFunc: PartialFunction[Any, Boolean] = {
    case longValue: Long if isSet => longValue == min
  }

  override protected def isEqualToMaxFunc: PartialFunction[Any, Boolean] = {
    case longValue: Long if isSet => longValue == max
  }

  override def getMin(): Any = if (isSet) min else null

  override def getMax(): Any = if (isSet) max else null
}

/**
 * [[StringColumnStatistics]] keep track of min/max/nulls for Binary column, which is a UTF-8
 * string. This does not support generic Binary statistics from Parquet.
 */
case class StringColumnStatistics() extends ColumnStatistics {
  private var min: String = null
  private var max: String = null
  private var isSet: Boolean = false

  override protected def updateMinMaxFunc: PartialFunction[Any, Unit] = {
    case strValue: String => {
      if (!isSet) {
        min = strValue
        max = strValue
        isSet = true
      } else {
        if (min > strValue) min = strValue
        if (max < strValue) max = strValue
      }
    }
  }

  override protected def containsFunc: PartialFunction[Any, Boolean] = {
    case strValue: String if isSet => strValue >= min && strValue <= max
  }

  override protected def isLessThanMinFunc: PartialFunction[Any, Boolean] = {
    case strValue: String if isSet => strValue < min
  }

  override protected def isGreaterThanMaxFunc: PartialFunction[Any, Boolean] = {
    case strValue: String if isSet => strValue > max
  }

  override protected def isEqualToMinFunc: PartialFunction[Any, Boolean] = {
    case strValue: String if isSet => strValue == min
  }

  override protected def isEqualToMaxFunc: PartialFunction[Any, Boolean] = {
    case strValue: String if isSet => strValue == max
  }

  override def getMin(): Any = min

  override def getMax(): Any = max
}

/**
 * [[DateColumnStatistics]] keep track of min/max/nulls for Date column, which is a Parquet int32
 * field, but converted into java.sql.Date instance in record container.
 */
case class DateColumnStatistics() extends ColumnStatistics {
  private var min: java.sql.Date = null
  private var max: java.sql.Date = null
  private var isSet: Boolean = false

  override protected def updateMinMaxFunc: PartialFunction[Any, Unit] = {
    case dateValue: java.sql.Date => {
      if (!isSet) {
        min = dateValue
        max = dateValue
        isSet = true
      } else {
        if (min.after(dateValue)) min = dateValue
        if (max.before(dateValue)) max = dateValue
      }
    }
  }

  override protected def containsFunc: PartialFunction[Any, Boolean] = {
    case dateValue: java.sql.Date if isSet => {
      dateValue.after(min) && dateValue.before(max) ||
        dateValue.equals(min) || dateValue.equals(max)
    }
  }

  override protected def isLessThanMinFunc: PartialFunction[Any, Boolean] = {
    case dateValue: java.sql.Date if isSet => dateValue.before(min)
  }

  override protected def isGreaterThanMaxFunc: PartialFunction[Any, Boolean] = {
    case dateValue: java.sql.Date if isSet => dateValue.after(max)
  }

  override protected def isEqualToMinFunc: PartialFunction[Any, Boolean] = {
    case dateValue: java.sql.Date if isSet => dateValue.equals(min)
  }

  override protected def isEqualToMaxFunc: PartialFunction[Any, Boolean] = {
    case dateValue: java.sql.Date if isSet => dateValue.equals(max)
  }

  override def getMin(): Any = if (isSet) min else null

  override def getMax(): Any = if (isSet) max else null
}

/**
 * [[TimestampColumnStatistics]] keep track of min/max/nulls for timestamp column, which is int96
 * in Parquet, but passed into statistics as java.sql.Timestamp value. See `RecordContainer` for
 * more information.
 */
case class TimestampColumnStatistics() extends ColumnStatistics {
  private var min: java.sql.Timestamp = null
  private var max: java.sql.Timestamp = null
  private var isSet: Boolean = false

  // When reading Parquet file values are stored as int96, which returned by container as Binary
  override protected def updateMinMaxFunc: PartialFunction[Any, Unit] = {
    case timeValue: java.sql.Timestamp => {
      if (!isSet) {
        min = timeValue
        max = timeValue
        isSet = true
      } else {
        if (min.after(timeValue)) min = timeValue
        if (max.before(timeValue)) max = timeValue
      }
    }
  }

  override protected def containsFunc: PartialFunction[Any, Boolean] = {
    case timeValue: java.sql.Timestamp if isSet => {
      timeValue.after(min) && timeValue.before(max) ||
        timeValue.equals(min) || timeValue.equals(max)
    }
  }

  override protected def isLessThanMinFunc: PartialFunction[Any, Boolean] = {
    case timeValue: java.sql.Timestamp if isSet => timeValue.before(min)
  }

  override protected def isGreaterThanMaxFunc: PartialFunction[Any, Boolean] = {
    case timeValue: java.sql.Timestamp if isSet => timeValue.after(max)
  }

  override protected def isEqualToMinFunc: PartialFunction[Any, Boolean] = {
    case timeValue: java.sql.Timestamp if isSet => timeValue.equals(min)
  }

  override protected def isEqualToMaxFunc: PartialFunction[Any, Boolean] = {
    case timeValue: java.sql.Timestamp if isSet => timeValue.equals(max)
  }

  override def getMin(): Any = if (isSet) min else null

  override def getMax(): Any = if (isSet) max else null
}
