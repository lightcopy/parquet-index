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

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BinaryRangeTreeSuite {
  @Test
  public void testInit() {
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>();
    assertEquals(tree.getMaxDepth(), BinaryRangeTree.DEFAULT_MAX_DEPTH);
    assertEquals(tree.getNumNulls(), 0);
    assertNull(tree.getMin());
    assertNull(tree.getMax());
  }

  @Test
  public void testInitFailWithSmallMaxDepth() {
    boolean error = false;
    try {
      BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>(0);
    } catch (IllegalArgumentException err) {
      assertEquals(err.getMessage(), "maxDepth is out of range, 0");
      error = true;
    }
    assertTrue(error);
  }

  @Test
  public void testInitFailWithLargeMaxDepth() {
    boolean error = false;
    try {
      BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>(21);
    } catch (IllegalArgumentException err) {
      assertEquals(err.getMessage(), "maxDepth is out of range, 21");
      error = true;
    }
    assertTrue(error);
  }

  @Test
  public void testToString() {
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>();
    assertEquals(tree.toString(), "BinaryRangeTree(maxDepth=" +
      BinaryRangeTree.DEFAULT_MAX_DEPTH + ", numNulls=0, root=null)");
  }

  @Test
  public void testInsertNulls() {
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>();
    assertEquals(tree.getNumNulls(), 0);
    for (int i = 0; i < 10; i++) {
      tree.insert(null);
    }
    assertEquals(tree.getNumNulls(), 10);
  }

  @Test
  public void testContainsNull() {
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>();
    assertFalse(tree.mightContain(null));
    tree.insert(null);
    assertTrue(tree.mightContain(null));
  }

  @Test
  public void testSortedArray() {
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>();
    for (int i = 1; i <= 100; i++) {
      tree.insert(i);
    }

    for (int i = 1; i <= 100; i++) {
      assertTrue(tree.mightContain(i));
    }

    for (int i = 101; i < 110; i++) {
      assertFalse(tree.mightContain(i));
    }

    for (int i = -10; i < 1; i++) {
      assertFalse(tree.mightContain(i));
    }

    assertTrue(tree.getMin().equals(1));
    assertTrue(tree.getMax().equals(100));
  }

  @Test
  public void testArray() {
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>();
    int[] arr = new int[] {4, 10, 12, 8, 6, -2, 2, -5, 14, 15};
    for (int elem : arr) {
      tree.insert(elem);
    }

    for (int elem : arr) {
      assertTrue(tree.mightContain(elem));
    }

    assertTrue(tree.getMin().equals(-5));
    assertTrue(tree.getMax().equals(15));
  }

  @Test
  public void testMinMax() {
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>();
    tree.insert(Integer.MIN_VALUE);
    tree.insert(Integer.MAX_VALUE);

    assertTrue(tree.getMin().equals(Integer.MIN_VALUE));
    assertTrue(tree.getMax().equals(Integer.MAX_VALUE));

    assertTrue(tree.mightContain(Integer.MIN_VALUE));
    assertTrue(tree.mightContain(Integer.MAX_VALUE));

    assertFalse(tree.mightContain(-50));
    assertFalse(tree.mightContain(-1));
    assertFalse(tree.mightContain(0));
    assertFalse(tree.mightContain(1));
    assertFalse(tree.mightContain(50));
  }

  @Test
  public void testMaxDepthTruncation() {
    BinaryRangeTree<String> tree = new BinaryRangeTree<String>(2);
    tree.insert("a");
    tree.insert("b");
    tree.insert("d");

    assertTrue(tree.mightContain("a"));
    assertTrue(tree.mightContain("b"));
    assertTrue(tree.mightContain("c"));
    assertTrue(tree.mightContain("d"));

    // increase max depth and run checks again, now "c" should be false, since
    // tree is not truncated and we can store all values
    tree = new BinaryRangeTree<String>(4);
    tree.insert("a");
    tree.insert("b");
    tree.insert("d");

    assertTrue(tree.mightContain("a"));
    assertTrue(tree.mightContain("b"));
    assertFalse(tree.mightContain("c"));
    assertTrue(tree.mightContain("d"));
  }

  @Test
  public void testMaxDepthTruncation2() {
    // insert values: 5, -2, 100; 10, 4; 30;
    BinaryRangeTree<Integer> tree = new BinaryRangeTree<Integer>(3);
    assertFalse(tree.mightContain(5));
    assertFalse(tree.mightContain(-2));
    assertFalse(tree.mightContain(100));

    // insert level 1 and 2
    tree.insert(5);
    tree.insert(-2);
    tree.insert(100);

    assertTrue(tree.mightContain(5));
    assertTrue(tree.mightContain(-2));
    assertTrue(tree.mightContain(100));

    assertFalse(tree.mightContain(99));
    assertFalse(tree.mightContain(0));

    // insert level 3
    tree.insert(10);
    tree.insert(4);
    assertTrue(tree.mightContain(10));
    assertTrue(tree.mightContain(4));

    assertFalse(tree.mightContain(6));
    assertFalse(tree.mightContain(99));

    // insert level 4 (truncated)
    tree.insert(30);
    assertTrue(tree.mightContain(30));
    // would exist, though we did not insert it - because of the truncation
    assertTrue(tree.mightContain(20));
    assertTrue(tree.mightContain(11));
  }
}
