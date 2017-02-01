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

public class ByteTrie {
  public static final int BYTE_LEAF_MASK = 1;
  public static final int INT_LEAF_MASK = 2;
  public static final int LONG_LEAF_MASK = 4;
  public static final int BINARY_LEAF_MASK = 8;

  private ByteTrieNode root;
  private boolean hasEmptyString;

  public static class ByteTrieNode {
    private int leaf;
    private byte value;
    private ByteTrieTable bytes;

    ByteTrieNode() {
      this.leaf = 0;
      this.bytes = null;
    }

    public ByteTrieNode(byte value) {
      this();
      this.value = value;
    }

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

    private ByteTrieNode getByte(byte value) {
      if (this.bytes == null) return null;
      return this.bytes.get(value);
    }

    private void markByteLeaf() {
      this.leaf |= BYTE_LEAF_MASK;
    }

    private void markIntLeaf() {
      this.leaf |= INT_LEAF_MASK;
    }

    private void markLongLeaf() {
      this.leaf |= LONG_LEAF_MASK;
    }

    private void markBinaryLeaf() {
      this.leaf |= BINARY_LEAF_MASK;
    }

    public void putByte(byte value) {
      ByteTrieNode node = insertByte(value);
      node.markByteLeaf();
    }

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

    public boolean containsByte(byte value) {
      ByteTrieNode node = getByte(value);
      return node != null && (node.leaf & BYTE_LEAF_MASK) > 0;
    }

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

  public static class ByteTrieTable {
    private final static int INITIAL_SIZE = 16;
    private final static int MAX_SIZE = 256;

    private int size = INITIAL_SIZE;
    private ByteTrieNode[] map;
    private int numInserted;

    ByteTrieTable(int initialSize) {
      assert initialSize >= INITIAL_SIZE && initialSize <= MAX_SIZE:
        "Invalid table size " + initialSize;
      this.size = initialSize;
      this.map = new ByteTrieNode[initialSize];
      this.numInserted = 0;
    }

    public ByteTrieTable() {
      this(INITIAL_SIZE);
    }

    public boolean put(ByteTrieNode node) {
      if (!insert(this.map, node)) {
        resize();
        return insert(this.map, node);
      }
      return false;
    }

    public ByteTrieNode get(byte value) {
      return lookup(this.map, value);
    }

    public int size() {
      return this.size;
    }

    public int numInserted() {
      return this.numInserted;
    }

    public boolean isEmpty() {
      return this.numInserted == 0;
    }

    // key does not check if value is out of range
    private int unsignedKey(byte rawKey) {
      return (1 << 8) - 1 & rawKey;
    }

    protected boolean insert(ByteTrieNode[] table, ByteTrieNode node) {
      if (size < MAX_SIZE) {
        return insertHash(table, node);
      } else {
        return insertDirect(table, node);
      }
    }

    private boolean insertHash(ByteTrieNode[] table, ByteTrieNode node) {
      // unsigned byte value
      int index = unsignedKey(node.value);
      int ref;
      for (int i = 0; i < size; i++) {
        ref = (index + i * (i + 1) / 2) % size;
        if (table[ref] == null) {
          numInserted++;
          table[ref] = node;
          return true;
        }
      }
      return false;
    }

    // always overwrite previous values in the table
    private boolean insertDirect(ByteTrieNode[] table, ByteTrieNode node) {
      int index = unsignedKey(node.value);
      if (table[index] == null) {
        numInserted++;
      }
      table[index] = node;
      return true;
    }

    protected ByteTrieNode lookup(ByteTrieNode[] table, byte key) {
      if (size < MAX_SIZE) {
        return lookupHash(table, key);
      } else {
        return lookupDirect(table, key);
      }
    }

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

    private ByteTrieNode lookupDirect(ByteTrieNode[] table, byte key) {
      return table[unsignedKey(key)];
    }

    protected void resize() {
      // do not grow beyond max size
      if (size >= MAX_SIZE) return;
      size <<= 2;
      numInserted = 0;
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

  public void update(byte value) {
    this.root.putByte(value);
  }

  public void update(int value) {
    this.root.putInt(value);
  }

  public void update(long value) {
    this.root.putLong(value);
  }

  public void update(String value) {
    if (value == null) return;
    if (value.isEmpty()) {
      this.hasEmptyString = true;
    } else {
      this.root.putBinary(value.getBytes());
    }
  }

  public boolean contains(byte value) {
    return this.root.containsByte(value);
  }

  public boolean contains(int value) {
    return this.root.containsInt(value);
  }

  public boolean contains(long value) {
    return this.root.containsLong(value);
  }

  public boolean contains(String value) {
    if (value == null) return false;
    if (value.isEmpty()) return this.hasEmptyString;
    return this.root.containsBinary(value.getBytes());
  }

  public ByteTrieNode getRoot() {
    return this.root;
  }

  @Override
  public String toString() {
    return "ByteTrie(root=" + this.root + ", hasEmptyString=" + this.hasEmptyString + ")";
  }
}
