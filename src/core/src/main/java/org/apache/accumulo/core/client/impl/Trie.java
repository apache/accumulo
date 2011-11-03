/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.impl;

import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;

public class Trie<VT> {
  
  private int maxDepth = 3;
  
  abstract class TrieNode<V> {
    protected int depth;
    
    abstract void add(ByteSequence key, V value);
    
    abstract V remove(ByteSequence key);
    
    abstract public boolean isEmpty();
    
    abstract Entry<ByteSequence,V> findMin();
    
    abstract Entry<ByteSequence,V> ceilingEntry(ByteSequence text, TrieNode<V> pnext);
    
    abstract void printInfo(Text prefix);
    // abstract Iterator<V> iterator(ByteSequence start);
    
  }
  
  static class TrieEntry<V> implements Entry<ByteSequence,V> {
    
    private ByteSequence key;
    private V value;
    
    TrieEntry(ByteSequence key, V val) {
      this.key = key;
      this.value = val;
    }
    
    @Override
    public ByteSequence getKey() {
      return key;
    }
    
    @Override
    public V getValue() {
      return value;
    }
    
    @Override
    public V setValue(V value) {
      V ret = this.value;
      this.value = value;
      return ret;
    }
    
  }
  
  @SuppressWarnings("unchecked")
  class InnerNode<V> extends TrieNode<V> {
    
    private TrieNode<V>[] children = new TrieNode[256];
    private TrieNode<V>[] next = new TrieNode[256];
    
    private int minIndex = 256;
    private int maxIndex = -1;
    private Entry<ByteSequence,V> entry;
    
    public InnerNode(int d) {
      this.depth = d;
    }
    
    @Override
    void add(ByteSequence key, V value) {
      
      if (depth == key.length()) {
        this.entry = new TrieEntry<V>(key, value);
        return;
      }
      
      int index = 0xff & key.byteAt(depth);
      
      if (children[index] == null) {
        if (depth + 1 < maxDepth)
          children[index] = new InnerNode<V>(depth + 1);
        else
          children[index] = new LeafNode<V>(depth + 1);
        
        if (minIndex == 256) {
          next[index] = children[index];
        } else if (index < minIndex) {
          int findex = index + 1;
          while (findex < minIndex)
            next[findex++] = children[minIndex];
          
          next[index] = children[index];
        } else if (index > maxIndex) {
          int findex = maxIndex + 1;
          while (findex < index)
            next[findex++] = children[index];
          
          next[index] = children[index];
        } else {
          if (next[index] == null)
            throw new IllegalStateException("next[" + index + "] == null");
          
          int bindex = index - 1;
          while (children[bindex] == null)
            next[bindex--] = children[index];
          
          int findex = index + 1;
          while (children[bindex] == null)
            next[findex++] = next[index];
          
          next[index] = children[index];
        }
        
        minIndex = Math.min(minIndex, index);
        maxIndex = Math.max(maxIndex, index);
      }
      
      children[index].add(key, value);
    }
    
    @Override
    V remove(ByteSequence key) {
      if (depth == key.length()) {
        V ret = entry.getValue();
        this.entry = null;
        return ret;
      }
      
      int index = 0xff & key.byteAt(depth);
      if (children[index] != null) {
        V ret = children[index].remove(key);
        if (children[index].isEmpty()) {
          if (next[index] != children[index])
            throw new IllegalStateException("next[" + index + "] != children[" + index + "]");
          
          if (index == minIndex && index == maxIndex) {
            minIndex = 256;
            maxIndex = -1;
            next[index] = null;
          } else if (index == minIndex) {
            int findex = index + 1;
            while (children[findex] == null) {
              next[findex] = null;
              findex++;
            }
            
            minIndex = findex;
            next[index] = null;
          } else if (index == maxIndex) {
            int bindex = index - 1;
            while (children[bindex] == null) {
              next[bindex] = null;
              bindex--;
            }
            maxIndex = bindex;
            next[index] = null;
          } else {
            next[index] = children[index + 1];
            int bindex = index - 1;
            while (children[bindex] == null) {
              next[bindex] = children[index + 1];
              bindex--;
            }
          }
          
          children[index] = null;
        }
        
        return ret;
      }
      
      return null;
    }
    
    @Override
    Entry<ByteSequence,V> ceilingEntry(ByteSequence text, TrieNode<V> pnext) {
      if (depth < text.length()) {
        int index = 0xff & text.byteAt(depth);
        
        if (index > maxIndex)
          return pnext.findMin();
        
        if (index < minIndex)
          return children[minIndex].findMin();
        
        TrieNode<V> child = children[index];
        
        if (index == maxIndex)
          return child.ceilingEntry(text, pnext);
        
        TrieNode<V> nextChild = next[index + 1];
        
        if (child != null)
          return child.ceilingEntry(text, nextChild);
        
        return nextChild.findMin();
        
      } else if (entry != null) {
        return entry;
      } else {
        return children[minIndex].findMin();
      }
    }
    
    @Override
    Entry<ByteSequence,V> findMin() {
      if (entry != null)
        return entry;
      
      if (minIndex == 256)
        return null;
      
      return children[minIndex].findMin();
    }
    
    @Override
    void printInfo(Text prefix) {
      // System.out.println("printInfo depth="+depth+" prefix="+prefix+" minIndex="+minIndex+" maxIndex="+maxIndex);
      for (int i = minIndex; i <= maxIndex; i++) {
        if (children[i] != null) {
          Text p = new Text(prefix);
          p.append(new byte[] {(byte) i}, 0, 1);
          children[i].printInfo(p);
        }
      }
    }
    
    @Override
    public boolean isEmpty() {
      return minIndex == 256 && entry == null;
    }
    
  }
  
  class LeafNode<V> extends TrieNode<V> {
    
    TreeMap<ByteSequence,V> map;
    
    public LeafNode(int d) {
      this.depth = d;
      map = new TreeMap<ByteSequence,V>();
    }
    
    @Override
    void add(ByteSequence key, V value) {
      map.put(key, value);
    }
    
    @Override
    Entry<ByteSequence,V> ceilingEntry(ByteSequence text, TrieNode<V> pnext) {
      Entry<ByteSequence,V> entry = map.ceilingEntry(text);
      if (entry != null)
        return entry;
      return pnext.findMin();
    }
    
    @Override
    Entry<ByteSequence,V> findMin() {
      Entry<ByteSequence,V> entry = map.firstEntry();
      if (entry != null)
        return entry;
      
      return null;
    }
    
    @Override
    void printInfo(Text prefix) {
      System.out.println(prefix + " " + map.size());
    }
    
    @Override
    public boolean isEmpty() {
      return map.size() == 0;
    }
    
    @Override
    V remove(ByteSequence key) {
      return map.remove(key);
    }
    
  }
  
  class NullNode<V> extends TrieNode<V> {
    
    @Override
    void add(ByteSequence key, V value) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    Entry<ByteSequence,V> ceilingEntry(ByteSequence text, TrieNode<V> pnext) {
      return null;
    }
    
    @Override
    Entry<ByteSequence,V> findMin() {
      return null;
    }
    
    @Override
    void printInfo(Text prefix) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isEmpty() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    V remove(ByteSequence key) {
      throw new UnsupportedOperationException();
    }
    
  }
  
  private TrieNode<VT> searchNode = new NullNode<VT>();
  private TrieNode<VT> rootNode = new InnerNode<VT>(0);
  
  public Trie(int maxDepth) {
    this.maxDepth = maxDepth;
  }
  
  public void add(Text key, VT value) {
    if (value == null)
      throw new IllegalArgumentException();
    rootNode.add(new ArrayByteSequence(key.getBytes(), 0, key.getLength()), value);
  }
  
  public void add(String key, VT value) {
    rootNode.add(new ArrayByteSequence(key), value);
  }
  
  public VT ceiling(ByteSequence key) {
    Entry<ByteSequence,VT> entry = rootNode.ceilingEntry(key, searchNode);
    if (entry == null)
      return null;
    return entry.getValue();
  }
  
  public VT ceiling(Text key) {
    return ceiling(new ArrayByteSequence(key.getBytes(), 0, key.getLength()));
  }
  
  VT ceiling(String key) {
    return ceiling(new Text(key));
  }
  
  VT remove(ByteSequence key) {
    return rootNode.remove(key);
  }
  
  public void printInfo() {
    rootNode.printInfo(new Text());
  }
  
  public static void main(String[] args) {
    Trie<String> trie1 = new Trie<String>(3);
    
    trie1.add("", "v4");
    trie1.add("b", "v1");
    trie1.add("bc", "v2");
    trie1.add("d", "v3");
    
    System.out.println(trie1.ceiling("") + " V4");
    System.out.println(trie1.ceiling("\0") + " V1");
    System.out.println(trie1.ceiling("a") + " V1");
    System.out.println(trie1.ceiling("b") + " V1");
    System.out.println(trie1.ceiling("b\0") + " V2");
    System.out.println(trie1.ceiling("b\0\0") + " V2");
    System.out.println(trie1.ceiling("b\0\0\0") + " V2");
    System.out.println(trie1.ceiling("ba") + " V2");
    System.out.println(trie1.ceiling("bc") + " V2");
    System.out.println(trie1.ceiling("bcc") + " V3");
    System.out.println(trie1.ceiling("bd") + " V3");
    System.out.println(trie1.ceiling("c") + " V3");
    System.out.println(trie1.ceiling("d") + " V3");
    System.out.println(trie1.ceiling("da") + " null");
  }
  
}
