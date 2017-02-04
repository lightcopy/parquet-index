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
import static org.junit.Assert.assertTrue;

import com.github.lightcopy.util.ByteTrie.ByteTrieNode;
import com.github.lightcopy.util.ByteTrie.ByteTrieTable;

/**
 * Test suite for ByteTrie, ByteTrieNode and ByteTrieTable.
 */
public class ByteTrieSuite {
  @Test
  public void testLeafMasks() {
    // All leaf masks should be different and powers of 2
    // It is important to keep them as these values.
    assertEquals(ByteTrie.BYTE_LEAF_MASK, 1);
    assertEquals(ByteTrie.INT_LEAF_MASK, 2);
    assertEquals(ByteTrie.LONG_LEAF_MASK, 4);
    assertEquals(ByteTrie.BINARY_LEAF_MASK, 8);
  }

  @Test
  public void testByteInsert() {
    ByteTrie trie = new ByteTrie();
    trie.put((byte) -1);
    trie.put((byte) 0);
    trie.put((byte) 1);
    trie.put(Byte.MAX_VALUE);
    trie.put(Byte.MIN_VALUE);

    // contains values
    assertTrue(trie.contains((byte) -1));
    assertTrue(trie.contains((byte) 0));
    assertTrue(trie.contains((byte) 1));
    assertTrue(trie.contains(Byte.MIN_VALUE));
    assertTrue(trie.contains(Byte.MAX_VALUE));
    // does not contain values
    assertFalse(trie.contains(-1));
    assertFalse(trie.contains(0));
    assertFalse(trie.contains(1));
    assertFalse(trie.contains(Integer.MIN_VALUE));
    assertFalse(trie.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testIntegerInsert() {
    ByteTrie trie = new ByteTrie();
    trie.put(0);
    trie.put(1);
    trie.put(Short.MIN_VALUE);
    trie.put(Short.MAX_VALUE);
    trie.put(Integer.MIN_VALUE);
    trie.put(Integer.MAX_VALUE);

    // contains values
    assertTrue(trie.contains(0));
    assertTrue(trie.contains(1));
    assertTrue(trie.contains(Short.MIN_VALUE));
    assertTrue(trie.contains(Short.MAX_VALUE));
    assertTrue(trie.contains(Integer.MIN_VALUE));
    assertTrue(trie.contains(Integer.MAX_VALUE));
    // does not contain values
    assertFalse(trie.contains(-1));
    assertFalse(trie.contains(-128));
    assertFalse(trie.contains(Short.MIN_VALUE + 1));
    assertFalse(trie.contains(Short.MAX_VALUE - 1));
    assertFalse(trie.contains(Integer.MIN_VALUE + 1));
    assertFalse(trie.contains(Integer.MAX_VALUE - 1));
  }

  @Test
  public void testLongInsert() {
    ByteTrie trie = new ByteTrie();
    trie.put(-1L);
    trie.put(0L);
    trie.put(1L);
    trie.put(Long.MIN_VALUE);
    trie.put(Long.MAX_VALUE);

    // contains values
    assertTrue(trie.contains(-1L));
    assertTrue(trie.contains(0L));
    assertTrue(trie.contains(1L));
    assertTrue(trie.contains(Long.MIN_VALUE));
    assertTrue(trie.contains(Long.MAX_VALUE));
    // does not contain values
    assertFalse(trie.contains(-1));
    assertFalse(trie.contains(0));
    assertFalse(trie.contains(1));
    assertFalse(trie.contains(Integer.MIN_VALUE));
    assertFalse(trie.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testInsertString() {
    ByteTrie trie = new ByteTrie();
    trie.put(null);
    trie.put("");
    trie.put("abc");
    trie.put("1024");
    trie.put("    ");
    trie.put("-1024");
    trie.put("" + Integer.MIN_VALUE);
    trie.put("" + Integer.MAX_VALUE);

    // contains value
    assertTrue(trie.contains(""));
    assertTrue(trie.contains("abc"));
    assertTrue(trie.contains("1024"));
    assertTrue(trie.contains("    "));
    assertTrue(trie.contains("-1024"));
    assertTrue(trie.contains("" + Integer.MIN_VALUE));
    assertTrue(trie.contains("" + Integer.MAX_VALUE));
    // does not contain values
    assertFalse(trie.contains(null));
    assertFalse(trie.contains(1024));
    assertFalse(trie.contains(-1024));
    assertFalse(trie.contains(Integer.MIN_VALUE));
    assertFalse(trie.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testDuplicateValues() {
    ByteTrie trie = new ByteTrie();
    trie.put(null);
    trie.put(null);
    trie.put("");
    trie.put("");
    trie.put(1024);
    trie.put(1024);
    trie.put(-1024);
    trie.put(-1024);
    trie.put(Integer.MIN_VALUE);
    trie.put(Integer.MIN_VALUE);
    trie.put(Integer.MAX_VALUE);
    trie.put(Integer.MAX_VALUE);

    assertFalse(trie.contains(null));
    assertTrue(trie.contains(""));
    assertTrue(trie.contains(1024));
    assertTrue(trie.contains(-1024));
    assertTrue(trie.contains(Integer.MIN_VALUE));
    assertTrue(trie.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testSameByteSequence() {
    ByteTrie trie = new ByteTrie();
    trie.put("abcd");
    // 97 + (98 << 8) + (99 << 16) + (100 << 24)
    trie.put(1684234849);

    assertTrue(trie.contains("abcd"));
    assertTrue(trie.contains(1684234849));
    assertFalse(trie.contains(1684234849L));
  }

  // == ByteTrieTable tests ==
  @Test
  public void testTableMaxSize() {
    assertEquals(ByteTrieTable.MAX_SIZE, 256);
  }

  @Test
  public void testTableInit() {
    ByteTrieTable table = new ByteTrieTable();
    assertEquals(table.size(), ByteTrieTable.INITIAL_SIZE);
    assertEquals(table.numInserted(), 0);
  }

  @Test
  public void testTableHashInsert() {
    ByteTrieTable table = new ByteTrieTable();
    assertEquals(table.numInserted(), 0);
    table.put(new ByteTrieNode((byte) 1));
    table.put(new ByteTrieNode((byte) 2));
    table.put(new ByteTrieNode((byte) 3));
    assertEquals(table.numInserted(), 3);

  }

  @Test
  public void testTableHashInsertDuplicates() {
    ByteTrieTable table = new ByteTrieTable();
    assertEquals(table.numInserted(), 0);
    table.put(new ByteTrieNode((byte) 1));
    table.put(new ByteTrieNode((byte) 1));
    table.put(new ByteTrieNode((byte) 1));
    assertEquals(table.numInserted(), 1);
  }

  @Test
  public void testTableDirectInsert() {
    ByteTrieTable table = new ByteTrieTable(ByteTrieTable.MAX_SIZE);
    assertEquals(table.numInserted(), 0);
    table.put(new ByteTrieNode((byte) 1));
    table.put(new ByteTrieNode((byte) 2));
    table.put(new ByteTrieNode((byte) 3));
    assertEquals(table.numInserted(), 3);

  }

  @Test
  public void testTableDirectInsertDuplicates() {
    ByteTrieTable table = new ByteTrieTable(ByteTrieTable.MAX_SIZE);
    assertEquals(table.numInserted(), 0);
    table.put(new ByteTrieNode((byte) 1));
    table.put(new ByteTrieNode((byte) 1));
    table.put(new ByteTrieNode((byte) 1));
    assertEquals(table.numInserted(), 1);
  }

  @Test
  public void testTableIsEmpty() {
    ByteTrieTable table = new ByteTrieTable();
    assertTrue(table.isEmpty());
    table.put(new ByteTrieNode((byte) 1));
    assertFalse(table.isEmpty());
  }

  @Test
  public void testTableResize() {
    ByteTrieTable table = new ByteTrieTable();
    assertTrue(table.isEmpty());

    int numInserted = 0;
    for (int i = -128; i <= 127; i++) {
      if (i % 2 == 0) {
        table.put(new ByteTrieNode((byte) i));
        numInserted++;
      }
    }

    assertEquals(table.size(), ByteTrieTable.MAX_SIZE);
    assertEquals(table.numInserted(), numInserted);
    for (int i = -128; i <= 127; i++) {
      if (i % 2 == 0) {
        assertTrue(table.get((byte) i) != null);
      } else {
        assertTrue(table.get((byte) i) == null);
      }
    }
  }
}
