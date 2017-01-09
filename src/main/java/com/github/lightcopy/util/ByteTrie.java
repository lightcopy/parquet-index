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

  static class ByteTrieNode {
    private int leaf;
    private ByteTrieNode[] bytes;

    ByteTrieNode() {
      this.leaf = 0;
      this.bytes = null;
    }

    private ByteTrieNode insertByte(byte value) {
      int index = (1 << 8) - 1 & value;
      if (this.bytes == null) {
        // to include as unsigned bytes
        this.bytes = new ByteTrieNode[256];
      }

      if (this.bytes[index] == null) {
        this.bytes[index] = new ByteTrieNode();
      }

      return this.bytes[index];
    }

    private ByteTrieNode getByte(byte value) {
      int index = (1 << 8) - 1 & value;
      if (this.bytes == null) return null;
      return this.bytes[index];
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
      String values = (this.bytes == null) ? "null" : ("" + this.bytes.length);
      return "ByteNode(leaf=" + this.leaf + ", values=" + values + ")";
    }
  }

  ByteTrie() {
    this.root = new ByteTrieNode();
  }
}
