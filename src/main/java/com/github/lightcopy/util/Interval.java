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

abstract class Interval implements Serializable {
  // returns -1 if this distance is less than other, 0 if distances are the same, 1 if this distance
  // is larger than other - similar to compareTo method
  public abstract int compareDistance(Interval other);

  public abstract boolean contains(Object value);

  public abstract Interval withUpdate(Object value);

  public abstract Object getMin();

  public abstract Object getMax();

  @Override
  public String toString() {
    return "(" + getMin() + ", " + getMax() + ")";
  }

  public static Interval fromValue(Object value) {
    if (value instanceof Integer) {
      return new IntegerInterval((Integer) value);
    } else if (value instanceof Long) {
      return new LongInterval((Long) value);
    } else {
      throw new UnsupportedOperationException("Type of value " + value + "is not supported");
    }
  }
}
