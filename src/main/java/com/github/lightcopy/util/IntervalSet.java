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

package com.github.lightcopy.util;

import java.io.Serializable;
import java.util.Arrays;

public class IntervalSet<T> implements Serializable {
  public static final int MIN_NUM_INTERVALS = 1;
  public static final int MAX_NUM_INTERVALS = 5;
  public static final int DEFAULT_NUM_INTERVALS = 3;

  private int index;
  private Interval[] intervals;

  public IntervalSet(int numIntervals) {
    if (numIntervals < MIN_NUM_INTERVALS || numIntervals > MAX_NUM_INTERVALS) {
      throw new IllegalArgumentException("Invalid number of intervals: " + MIN_NUM_INTERVALS +
        " <= " + numIntervals + " <= " + MAX_NUM_INTERVALS);
    }
    this.intervals = new Interval[numIntervals];
    this.index = 0;
  }

  public IntervalSet() {
    this(DEFAULT_NUM_INTERVALS);
  }

  public boolean insert(T value) {
    if (this.index < this.intervals.length) {
      this.intervals[this.index++] = Interval.fromValue(value);
      return true;
    } else {
      // if value exists in one of the intervals, do not modify intervals
      for (Interval dst : this.intervals) {
        if (dst.contains(value)) {
          return false;
        }
      }

      // guarantee that we will have at least one interval
      // all intervals at this point do not contain value, meaning that value will always extend
      // one of the intervals - we need to find the smallest increase in distance
      if (this.intervals.length == 0) return false;
      // update the interval with smallest distance factor
      int minIntervalIndex = -1;
      Interval minInterval = null;
      for (int i = 0; i < this.intervals.length; i++) {
        Interval tmp = this.intervals[i].withUpdate(value);
        if (minInterval == null || minInterval.compareDistance(tmp) > 0) {
          minInterval = tmp;
          minIntervalIndex = i;
        }
      }
      this.intervals[minIntervalIndex] = minInterval;
      return true;
    }
  }

  public boolean contains(T value) {
    for (Interval dst : this.intervals) {
      if (dst != null && dst.contains(value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return Arrays.toString(this.intervals);
  }
}
