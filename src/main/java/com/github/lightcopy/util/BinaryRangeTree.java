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
import java.util.ArrayList;
import java.util.List;

public class BinaryRangeTree<T extends Comparable<T>> implements Serializable {
  static class TreeNode<T extends Comparable<T>> implements Serializable {
    T value;
    T min;
    T max;
    int height;
    TreeNode<T> left;
    TreeNode<T> right;

    public TreeNode(T value) {
      if (value == null) throw new NullPointerException();
      this.value = value;
      this.min = value;
      this.max = value;
      this.height = 0;
      this.left = null;
      this.right = null;
    }

    protected T minValue(T left, T right) {
      if (left.compareTo(right) < 0) return left;
      return right;
    }

    protected T maxValue(T left, T right) {
      if (left.compareTo(right) > 0) return left;
      return right;
    }

    public void updateMinMaxValue(T other) {
      this.min = minValue(this.min, other);
      this.max = maxValue(this.max, other);
    }

    @Override
    public String toString() {
      return "[value=" + this.value + ", height=" + this.height + ", min=" + this.min + ", max=" +
        this.max + "]";
    }
  }

  public static final int MAX_HEIGHT = 20;
  public static final int DEFAULT_HEIGHT = 10;

  private TreeNode<T> root;
  private final int maxHeight;
  private int numNulls;

  public BinaryRangeTree(int maxHeight) {
    if (maxHeight < 1 || maxHeight > MAX_HEIGHT) {
      throw new IllegalArgumentException("Invalid max height: " + maxHeight);
    }
    this.root = null;
    this.maxHeight = maxHeight;
  }

  public BinaryRangeTree() {
    this(DEFAULT_HEIGHT);
  }

  private int height(TreeNode<T> node) {
    return (node == null) ? -1 : node.height;
  }

  private int balanceFactor(TreeNode<T> node) {
    return height(node.left) - height(node.right);
  }

  private TreeNode<T> balance(TreeNode<T> node) {
    if (balanceFactor(node) < -1) {
      if (balanceFactor(node.right) > 0) {
        node.right = rotateRight(node.right);
      }
      node = rotateLeft(node);
    } else if (balanceFactor(node) > 1) {
      if (balanceFactor(node.left) < 0) {
        node.left = rotateLeft(node.left);
      }
      node = rotateRight(node);
    }
    return node;
  }

  private TreeNode<T> rotateRight(TreeNode<T> node) {
    TreeNode<T> tmp = node.left;
    node.left = tmp.right;
    tmp.right = node;
    node.height = 1 + Math.max(height(node.left), height(node.right));
    tmp.height = 1 + Math.max(height(tmp.left), height(tmp.right));
    // update statistics
    node.min = (node.left == null) ? node.value : node.left.min;
    tmp.max = tmp.right.max;
    return tmp;
  }

  private TreeNode<T> rotateLeft(TreeNode<T> node) {
    TreeNode<T> tmp = node.right;
    node.right = tmp.left;
    tmp.left = node;
    node.height = 1 + Math.max(height(node.left), height(node.right));
    tmp.height = 1 + Math.max(height(tmp.left), height(tmp.right));
    // update statistics
    node.max = (node.right == null) ? node.value : node.right.max;
    tmp.min = tmp.left.min;
    return tmp;
  }

  private TreeNode<T> insert(T value, TreeNode<T> node) {
    if (node == null) {
      if (hasMaxHeight()) return null;
      return new TreeNode<T>(value);
    }
    if (node.value.compareTo(value) == 0) return node;
    if (node.value.compareTo(value) > 0) {
      node.left = insert(value, node.left);
    } else {
      node.right = insert(value, node.right);
    }
    node.height = 1 + Math.max(height(node.left), height(node.right));
    node.updateMinMaxValue(value);
    return balance(node);
  }

  public void insert(T value) {
    if (value == null) {
      this.numNulls++;
    } else {
      this.root = insert(value, this.root);
    }
  }

  private boolean mightContain(T value, TreeNode<T> node) {
    if (node == null) return true;
    if (node.value.compareTo(value) == 0) return true;
    if (node.min.compareTo(value) > 0 || node.max.compareTo(value) < 0) return false;
    if (node.value.compareTo(value) > 0) {
      return mightContain(value, node.left);
    } else {
      return mightContain(value, node.right);
    }
  }

  public boolean mightContain(T value) {
    if (value == null) return this.numNulls > 0;
    if (!isSet()) return false;
    return mightContain(value, this.root);
  }

  private boolean contains(T value, TreeNode<T> node) {
    if (node == null) return false;
    if (node.value.compareTo(value) == 0) return true;
    if (node.value.compareTo(value) > 0) {
      return contains(value, node.left);
    } else {
      return contains(value, node.right);
    }
  }

  public boolean contains(T value) {
    if (value == null) return this.numNulls > 0;
    if (!isSet()) return false;
    return contains(value, this.root);
  }

  private boolean isBalanced(TreeNode<T> node) {
    if (node == null) return true;
    int factor = balanceFactor(node);
    if (factor > 1 || factor < -1) return false;
    return isBalanced(node.left) && isBalanced(node.right);
  }

  public boolean isBalanced() {
    return isBalanced(this.root);
  }

  private boolean isBST(TreeNode<T> node, T min, T max) {
    if (node == null) return true;
    if (min != null && node.value.compareTo(min) < 0) return false;
    if (max != null && node.value.compareTo(max) > 0) return false;
    return isBST(node.left, min, node.value) && isBST(node.right, node.value, max);
  }

  public boolean isBST() {
    return isBST(this.root, null, null);
  }

  private boolean hasValidStatistics(TreeNode<T> node, T min, T max) {
    if (node == null) return true;
    if (min != null && node.min.compareTo(min) < 0) return false;
    if (max != null && node.max.compareTo(max) > 0) return false;
    if (node.value.compareTo(node.min) < 0) return false;
    if (node.value.compareTo(node.max) > 0) return false;
    return hasValidStatistics(node.left, node.min, node.max) &&
      hasValidStatistics(node.right, node.min, node.max);
  }

  public boolean hasValidStatistics() {
    return hasValidStatistics(this.root, null, null);
  }

  public boolean isSet() {
    return this.root != null;
  }

  public boolean hasMaxHeight() {
    return isSet() && this.root.height >= this.maxHeight;
  }

  public int getMaxHeight() {
    return this.maxHeight;
  }

  public T getMin() {
    return isSet() ? this.root.min : null;
  }

  public T getMax() {
    return isSet() ? this.root.max : null;
  }

  public int getNumNulls() {
    return this.numNulls;
  }

  private String debug(String margin, TreeNode node) {
    if (node == null) {
      return margin + "null";
    } else {
      return margin + node + ": " + "\n" +
        debug(margin + "  ", node.left) + "\n" +
        debug(margin + "  ", node.right);
    }
  }

  public String debug() {
    return debug("", this.root);
  }

  @Override
  public String toString() {
    return "[init=" + isSet() +
      ", maxHeight=" + getMaxHeight() +
      ", hasMaxHeight=" + hasMaxHeight() +
      ", balanced=" + isBalanced() +
      ", bst=" + isBST() +
      ", stats=" + hasValidStatistics() +
      ", min=" + getMin() +
      ", max=" + getMax() +
      ", numNulls=" + getNumNulls() + "]";
  }
}
