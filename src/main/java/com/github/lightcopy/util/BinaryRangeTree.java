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

/**
 * [[BinaryRangeTree]] is a modified simple binary search tree to contain partial statistics for
 * value of T. Collects information about inserted nulls, global min/max and also tree-like
 * statistics. Currently only supports value lookup and insertion, does not support updates or
 * deletes, and is not self-balancing.
 * To maintain access and insertion performance tree can be bounded by maxDepth, which is maximum
 * number of levels to keep, while still maintaining min/max statistics for all inserted values.
 * Larger maxDepth provides less error on check if element in statistics, but gives more overhead
 * in terms of insert/lookup time as well as space.
 */
public class BinaryRangeTree<T extends Comparable<T>> {
  // default number of levels to keep, for perfectly balanced BST is 2^10 - 1 elements
  public static final int DEFAULT_MAX_DEPTH = 10;

  private TreeNode<T> root;
  private int maxDepth;
  private int numNulls;

  /**
   * BST TreeNode is null-intolerant, maintains min/max statistics and pointers to children.
   * provides methods to insert or look up element with potential false positives.
   */
  static class TreeNode<T extends Comparable<T>> {
    private T min;
    private T max;
    private T value;
    private TreeNode<T> left;
    private TreeNode<T> right;

    TreeNode(T value) {
      this.value = value;
      this.min = value;
      this.max = value;
    }

    /**
     * Insert elem for provided maxDepth. Statistics are always updated regardless maxDepth value.
     * Element to insert must not be null.
     * @param elem element to insert
     * @param maxDepth max depth to insert into noe
     */
    void insert(T elem, int maxDepth) {
      if (this.value.compareTo(elem) == 0) return;
      this.min = this.min.compareTo(elem) > 0 ? elem : this.min;
      this.max = this.max.compareTo(elem) < 0 ? elem : this.max;
      if (maxDepth <= 1) return;

      if (this.value.compareTo(elem) > 0) {
        if (this.left == null) {
          this.left = new TreeNode<T>(elem);
        } else {
          this.left.insert(elem, maxDepth - 1);
        }
      } else {
        if (this.right == null) {
          this.right = new TreeNode<T>(elem);
        } else {
          this.right.insert(elem, maxDepth - 1);
        }
      }
    }

    /**
     * Method to check if element is in statistics. Might return false positives, when level is
     * truncated by max depth for this node. Element to check must not be null.
     * @param elem element to check
     * @return true if element in statistics, false otherwise
     */
    boolean mightContain(T elem) {
      if (this.value.compareTo(elem) == 0) return true;
      if (this.min.compareTo(elem) > 0 || this.max.compareTo(elem) < 0) return false;
      if (this.value.compareTo(elem) > 0) {
        if (this.left == null) return true;
        return this.left.mightContain(elem);
      } else {
        if (this.right == null) return true;
        return this.right.mightContain(elem);
      }
    }

    @Override
    public String toString() {
      return "TreeNode(value=" + this.value + ", min=" + this.min + ", max=" + this.max +
        ", left=" + this.left + ", right=" + this.right + ")";
    }
  }

  public BinaryRangeTree(int maxDepth) {
    if (maxDepth < 1 || maxDepth > 20) {
      throw new IllegalArgumentException("maxDepth is out of range, " + maxDepth);
    }
    this.maxDepth = maxDepth;
    this.numNulls = 0;
    this.root = null;
  }

  /** Create tree with default depth */
  public BinaryRangeTree() {
    this(DEFAULT_MAX_DEPTH);
  }

  /**
   * Insert element into the tree, element can be null.
   * Nulls are maintained separately, and not added to the tree nodes.
   * @param elem element to insert, can be null
   */
  public void insert(T elem) {
    if (elem == null) {
      this.numNulls++;
    } else if (this.root == null) {
      this.root = new TreeNode<T>(elem);
    } else {
      this.root.insert(elem, this.maxDepth);
    }
  }

  /**
   * Check if element is in statistics. Might return false positive result depending on used
   * max depth per tree node, meaning that 100% guarantee if element is not in statistics, but
   * returns true if element is in statistics or falls in range. Element can be null, will check
   * null storage to answer query. Returns false if tree has not yet been initialized.
   * @param elem element to check
   * @return true if element in statistics, false otherwise
   */
  public boolean mightContain(T elem) {
    if (elem == null) return this.numNulls > 0;
    if (this.root == null) return false;
    return this.root.mightContain(elem);
  }

  /**
   * Get number of nulls accumulated so far.
   * @return number of nulls
   */
  public int getNumNulls() {
    return this.numNulls;
  }

  /**
   * Get max depth used by this tree.
   * @return max depth
   */
  public int getMaxDepth() {
    return this.maxDepth;
  }

  /**
   * Get global min value for the tree.
   * @return min value of T
   */
  public T getMin() {
    return this.root == null ? null : this.root.min;
  }

  /**
   * Get global max value for the tree.
   * @return max value of T
   */
  public T getMax() {
    return this.root == null ? null : this.root.max;
  }

  @Override
  public String toString() {
    return "BinaryRangeTree(maxDepth=" + this.maxDepth + ", numNulls=" + this.numNulls +
      ", root=" + this.root + ")";
  }
}
