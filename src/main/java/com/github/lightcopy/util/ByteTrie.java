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

import java.util.HashMap;

public class ByteTrie {
  public static final int BYTE_LEAF_MASK = 1;
  public static final int INT_LEAF_MASK = 2;
  public static final int LONG_LEAF_MASK = 4;
  public static final int BINARY_LEAF_MASK = 8;

  private ByteTrieNode root;
  private boolean hasEmptyString;

  static class ByteTrieNode {
    private int leaf;
    private HashMap<Integer, ByteTrieNode> bytes;

    ByteTrieNode() {
      this.leaf = 0;
      this.bytes = null;
    }

    private ByteTrieNode insertByte(byte value) {
      int index = (1 << 8) - 1 & value;
      if (this.bytes == null) {
        this.bytes = new HashMap<Integer, ByteTrieNode>();
      }
      if (!this.bytes.containsKey(index)) {
        this.bytes.put(index, new ByteTrieNode());
      }
      return this.bytes.get(index);
    }

    private ByteTrieNode getByte(byte value) {
      int index = (1 << 8) - 1 & value;
      if (this.bytes == null) return null;
      return this.bytes.get(index);
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
      return "ByteNode(leaf=" + this.leaf + ", values=" + this.bytes + ")";
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
