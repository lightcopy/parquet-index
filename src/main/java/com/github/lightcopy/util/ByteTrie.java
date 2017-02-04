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
 * Prefix tree to store primitive types and arrays of bytes efficiently and offer fast "exists"
 * lookups. Used for dictionary filtering.
 * To distinguish between different types masks are used for each trie node. Currently these types
 * are supported:
 * - Integer
 * - Long
 * - Byte
 * - Byte array
 */
public class ByteTrie {
  public static final int BYTE_LEAF_MASK = 1;
  public static final int INT_LEAF_MASK = 2;
  public static final int LONG_LEAF_MASK = 4;
  public static final int BINARY_LEAF_MASK = 8;

  // Root node to start traversal and insertions
  private ByteTrieNode root;
  // Whether or not trie contains any empty string instance; allows to look up empty strings
  private boolean hasEmptyString;

  /**
   * Byte trie node.
   * Each node contains resizable hash table for children, which is only allocated for the first
   * child node insertion. Each node contains additional pointer for label (value). If node is leaf,
   * meaning that there is a value that ends on this node, leaf property contains summary of all
   * valid masks.
   */
  static class ByteTrieNode {
    // leaf mask that includes all masks ending on this node, otherwise is 0
    private int leaf;
    // label/value for this node
    private byte value;
    // hash table for child nodes
    private ByteTrieTable bytes;

    ByteTrieNode() {
      this.leaf = 0;
      this.bytes = null;
    }

    ByteTrieNode(byte value) {
      this();
      this.value = value;
    }

    /**
     * Insert signed byte value into the trie node.
     * Returns newly created node or existing one for the value.
     */
    private ByteTrieNode insertByte(byte value) {
      if (this.bytes == null) {
        this.bytes = new ByteTrieTable();
      }
      ByteTrieNode node = this.bytes.get(value);
      if (node == null) {
        node = new ByteTrieNode(value);
        this.bytes.put(node);
      }
      return node;
    }

    /**
     * Return node for provided signed byte value.
     * Can return null, ff node is leaf or table does not have value.
     */
    private ByteTrieNode getByte(byte value) {
      if (this.bytes == null) return null;
      return this.bytes.get(value);
    }

    /** Mark current node as byte leaf node */
    private void markByteLeaf() {
      this.leaf |= BYTE_LEAF_MASK;
    }

    /** Mark current node as integer leaf node */
    private void markIntLeaf() {
      this.leaf |= INT_LEAF_MASK;
    }

    /** Mark current node as long leaf node */
    private void markLongLeaf() {
      this.leaf |= LONG_LEAF_MASK;
    }

    /** Mark current node as binary leaf node */
    private void markBinaryLeaf() {
      this.leaf |= BINARY_LEAF_MASK;
    }

    /** Insert single signed byte value */
    public void putByte(byte value) {
      ByteTrieNode node = insertByte(value);
      node.markByteLeaf();
    }

    /** Insert signed integer value, stores bytes in little endian format */
    public void putInt(int value) {
      ByteTrieNode node = this;
      if (value == 0) {
        node = node.insertByte((byte) 0);
      } else {
        byte part = 0;
        while (value != 0) {
          part = (byte) (value & (1 << 8) - 1);
          value = value >>> 8;
          node = node.insertByte(part);
        }
      }
      node.markIntLeaf();
    }

    /** Insert signed long value, stores bytes in little endian format */
    public void putLong(long value) {
      ByteTrieNode node = this;
      if (value == 0) {
        node = node.insertByte((byte) 0);
      } else {
        byte part = 0;
        while (value != 0) {
          part = (byte) (value & (1 << 8) - 1);
          value = value >>> 8;
          node = node.insertByte(part);
        }
      }
      node.markLongLeaf();
    }

    /**
     * Insert byte array, bytes are stored in order of array.
     * Does not support nulls or empty byte arrays.
     */
    public void putBinary(byte[] value) {
      if (value == null || value.length == 0) {
        throw new IllegalArgumentException("Binary null/empty value");
      }
      ByteTrieNode node = this;
      int index = 0;
      while (index < value.length) {
        node = node.insertByte(value[index]);
        index++;
      }
      node.markBinaryLeaf();
    }

    /** Check if trie contains byte value */
    public boolean containsByte(byte value) {
      ByteTrieNode node = getByte(value);
      return node != null && (node.leaf & BYTE_LEAF_MASK) > 0;
    }

    /** Check if trie contains signed integer value */
    public boolean containsInt(int value) {
      ByteTrieNode node = this;
      if (value == 0) {
        node = node.getByte((byte) 0);
      } else {
        byte part = 0;
        while (value != 0) {
          part = (byte) (value & (1 << 8) - 1);
          value = value >>> 8;
          node = node.getByte(part);
          if (node == null) return false;
        }
      }
      return node != null && (node.leaf & INT_LEAF_MASK) > 0;
    }

    /** Check if trie contains signed long value */
    public boolean containsLong(long value) {
      ByteTrieNode node = this;
      if (value == 0) {
        node = node.getByte((byte) 0);
      } else {
        byte part = 0;
        while (value != 0) {
          part = (byte) (value & (1 << 8) - 1);
          value = value >>> 8;
          node = node.getByte(part);
          if (node == null) return false;
        }
      }
      return node != null && (node.leaf & LONG_LEAF_MASK) > 0;
    }

    /** Check if trie contains byte array */
    public boolean containsBinary(byte[] value) {
      if (value == null || value.length == 0) {
        throw new IllegalArgumentException("Binary null/empty value");
      }
      ByteTrieNode node = this;
      int index = 0;
      while (index < value.length) {
        node = node.getByte(value[index]);
        if (node == null) return false;
        index++;
      }
      return node != null && (node.leaf & BINARY_LEAF_MASK) > 0;
    }

    @Override
    public String toString() {
      return "ByteNode(" + this.value + ", leaf=" + this.leaf + ", values=" + this.bytes + ")";
    }
  }

  /**
   * Resizable (powers of 2) open-addressing hash table with quadratic probing and triangular
   * lookup sequence. Has two modes, one is for hash table and when is sized to MAX_SIZE is
   * converted into direct index lookup without hash function.
   */
  public static class ByteTrieTable {
    public final static int INITIAL_SIZE = 16;
    public final static int MAX_SIZE = 256;

    private int size = INITIAL_SIZE;
    // underlying resizable array
    private ByteTrieNode[] map;
    // statistics for number of inserted elements
    private int numInserted;

    ByteTrieTable(int initialSize) {
      assert initialSize >= INITIAL_SIZE && initialSize <= MAX_SIZE:
        "Invalid table size " + initialSize;
      this.size = initialSize;
      this.map = new ByteTrieNode[initialSize];
      this.numInserted = 0;
    }

    ByteTrieTable() {
      this(INITIAL_SIZE);
    }

    /**
     * Insert ByteTrieNode node into table, use node value as a key.
     * Return boolean value to indicate whether or not insert was successful.
     */
    public boolean put(ByteTrieNode node) {
      if (!insert(this.map, node)) {
        resize();
        return insert(this.map, node);
      }
      return true;
    }

    /** Get node for provided signed byte value */
    public ByteTrieNode get(byte value) {
      return lookup(this.map, value);
    }

    /** Get size of underlying array, not number of inserted elements */
    public int size() {
      return this.size;
    }

    /** Number of inserted elements in the table */
    public int numInserted() {
      return this.numInserted;
    }

    /** Whether or not table is empty */
    public boolean isEmpty() {
      return this.numInserted == 0;
    }

    /**
     * Generate unsigned key, used as hash function.
     * Key is not checked if value is out of range.
     */
    private int unsignedKey(byte rawKey) {
      return (1 << 8) - 1 & rawKey;
    }

    /**
     * Generic insert of the node into table, depending on size of the table chooses either direct
     * insert by index or hash insert.
     */
    protected boolean insert(ByteTrieNode[] table, ByteTrieNode node) {
      if (size < MAX_SIZE) {
        return insertHash(table, node);
      } else {
        return insertDirect(table, node);
      }
    }

    /**
     * Make hash insert, e.g. generate key and insert/resolve collisions.
     * Duplicate values will be overwritten, similar to direct insert.
     */
    private boolean insertHash(ByteTrieNode[] table, ByteTrieNode node) {
      // unsigned byte value
      int index = unsignedKey(node.value);
      int ref;
      for (int i = 0; i < size; i++) {
        ref = (index + i * (i + 1) / 2) % size;
        if (table[ref] == null || table[ref].value == node.value) {
          if (table[ref] == null) {
            // only increment for newly inserted values
            numInserted++;
          }
          table[ref] = node;
          return true;
        }
      }
      return false;
    }

    /**
     * Make direct insert into the table. Duplicate values are overwritten, which is consistent
     * with hash lookup.
     */
    private boolean insertDirect(ByteTrieNode[] table, ByteTrieNode node) {
      int index = unsignedKey(node.value);
      if (table[index] == null) {
        numInserted++;
      }
      table[index] = node;
      return true;
    }

    /** Generic lookup of the key in provided table */
    protected ByteTrieNode lookup(ByteTrieNode[] table, byte key) {
      if (size < MAX_SIZE) {
        return lookupHash(table, key);
      } else {
        return lookupDirect(table, key);
      }
    }

    /**
     * Perform hash lookup. Note that since insert overwrites duplicate values, the last inserted
     * value will be returned in this case.
     */
    private ByteTrieNode lookupHash(ByteTrieNode[] table, byte key) {
      int index = unsignedKey(key);
      int ref;
      for (int i = 0; i < size; i++) {
        ref = (index + i * (i + 1) / 2) % size;
        if (table[ref] != null && table[ref].value == key) {
          return table[ref];
        }
      }
      return null;
    }

    /**
     * Perform direct lookup. Note that since insert overwrites duplicate values, the last inserted
     * value will be returned in this case.
     */
    private ByteTrieNode lookupDirect(ByteTrieNode[] table, byte key) {
      return table[unsignedKey(key)];
    }

    /**
     * Resize current map. If size is alreday maximum, then no-op. Resizing involves rehashing keys
     * and in this implementation we just reinsert values.
     * TODO: rehash keys without reinsertion.
     */
    protected void resize() {
      // do not grow table beyond max size
      if (size >= MAX_SIZE) return;
      size <<= 2;
      numInserted = 0; // reset numInserted, it will increment for each insert
      ByteTrieNode[] arr = new ByteTrieNode[size];
      for (int i = 0; i < this.map.length; i++) {
        if (this.map[i] != null) {
          insert(arr, this.map[i]);
        }
      }
      this.map = arr;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < this.map.length; i++) {
        if (i == 0) {
          sb.append(this.map[i]);
        } else {
          sb.append(", ");
          sb.append(this.map[i]);
        }
      }
      return "ByteTrieTable(size=" + this.size + ", numInserted=" + this.numInserted +
        ", values=["+ sb.toString() + "]";
    }
  }

  public ByteTrie() {
    this.root = new ByteTrieNode();
    this.hasEmptyString = false;
  }

  /** Insert signed byte value */
  public void put(byte value) {
    this.root.putByte(value);
  }

  /** Insert signed integer value */
  public void put(int value) {
    this.root.putInt(value);
  }

  /** Insert signed long value */
  public void put(long value) {
    this.root.putLong(value);
  }

  /**
   * Insert String instance as sequence of bytes.
   * Empty strings are allowed, but no-op for null values.
   */
  public void put(String value) {
    if (value == null) return;
    if (value.isEmpty()) {
      this.hasEmptyString = true;
    } else {
      this.root.putBinary(value.getBytes());
    }
  }

  /** Check if trie contains signed byte value */
  public boolean contains(byte value) {
    return this.root.containsByte(value);
  }

  /** Check if trie contains signed integer value */
  public boolean contains(int value) {
    return this.root.containsInt(value);
  }

  /** Check if trie contains signed long value */
  public boolean contains(long value) {
    return this.root.containsLong(value);
  }

  /** Check if trie contains String value */
  public boolean contains(String value) {
    if (value == null) return false;
    if (value.isEmpty()) return this.hasEmptyString;
    return this.root.containsBinary(value.getBytes());
  }

  ByteTrieNode getRoot() {
    return this.root;
  }

  @Override
  public String toString() {
    return "ByteTrie(root=" + this.root + ", hasEmptyString=" + this.hasEmptyString + ")";
  }
}
