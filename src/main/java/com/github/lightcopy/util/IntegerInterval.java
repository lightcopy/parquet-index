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

public class IntegerInterval extends Interval {
  private final Integer min;
  private final Integer max;

  IntegerInterval(Integer minValue, Integer maxValue) {
    if (minValue == null || maxValue == null || minValue.compareTo(maxValue) > 0) {
      throw new IllegalArgumentException("Invalid min/max: " + minValue + " <= " + maxValue);
    }
    this.min = minValue;
    this.max = maxValue;
  }

  IntegerInterval(Integer value) {
    this(value, value);
  }

  public int distance() {
    return Math.abs(this.max.intValue() - this.min.intValue());
  }

  @Override
  public int compareDistance(Interval other) {
    if (!(other instanceof IntegerInterval)) {
      throw new IllegalArgumentException("Interval " + other + " is not of type " + getClass());
    }
    IntegerInterval interval = (IntegerInterval) other;
    if (this.distance() < interval.distance()) {
      return -1;
    } else if (this.distance() > interval.distance()) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public boolean contains(Object value) {
    Integer other = (Integer) value;
    return other.compareTo(this.min) >= 0 && other.compareTo(this.max) <= 0;
  }

  @Override
  public Interval withUpdate(Object value) {
    Integer other = (Integer) value;
    if (other.compareTo(this.min) < 0) {
      return new IntegerInterval(other, this.max);
    } else {
      return new IntegerInterval(this.min, other);
    }
  }

  @Override
  public Object getMin() {
    return this.min;
  }

  @Override
  public Object getMax() {
    return this.max;
  }
}
