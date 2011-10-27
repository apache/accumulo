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
package org.apache.accumulo.core.security;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.util.ByteArrayComparator;

public class ByteArrayTrie {
  
  private ArrayList<Byte> objectTree;
  private byte[] tree;
  
  private static class State {
    int start;
    int size;
    boolean accept;
    boolean failed;
  }
  
  public ByteArrayTrie(Collection<byte[]> values) {
    create(values);
  }
  
  private void create(Collection<byte[]> values) {
    
    objectTree = new ArrayList<Byte>();
    // add a leaf state (shared)
    objectTree.add((byte) 0x0);
    objectTree.add((byte) 0x1);
    
    // create a sorted set of the byte[] values
    SortedSet<byte[]> sortedValues = new TreeSet<byte[]>(new ByteArrayComparator());
    
    sortedValues.addAll(values);
    
    // create the arraylist representation of the trie
    create(sortedValues, 1);
    
    // copy the arraylist representation to the byte[] representation
    tree = new byte[objectTree.size()];
    for (int i = 0; i < objectTree.size(); i++)
      tree[i] = objectTree.get(i);
    // free the reference to the ArrayList representation
    objectTree = null;
  }
  
  private final int indexBytes = 3;
  
  private void create(SortedSet<byte[]> values, int indexDepth) {
    // create a tree node
    // this method assumes that the byte[]s in values all have the same prefix up to indexDepth-1 bytes
    
    // partition the values into two sets: those with the same transition byte as the first record and those with other transition bytes
    SortedSet<byte[]> headValues = null;
    SortedSet<byte[]> tailValues = values;
    
    // calculate the size of this node from the number of unique transition bytes
    int numTransitions = 0;
    Byte lastByte = null;
    boolean accept = false;
    // figure out the number of transitions, and whether this state is accepting
    Iterator<byte[]> valueIter = tailValues.iterator();
    while (valueIter.hasNext()) {
      byte[] v = valueIter.next();
      if (v.length == indexDepth - 1) {
        // if there is an entry in values that requires no transitions from this state, then this is an accepting state
        accept = true;
        valueIter.remove();
        continue;
      }
      if (lastByte == null || lastByte.byteValue() != v[indexDepth - 1]) {
        lastByte = v[indexDepth - 1];
        numTransitions++;
      }
    }
    
    // start the first state
    int entryPosition = objectTree.size();
    // add space for the header
    objectTree.add((byte) numTransitions);
    if (accept)
      objectTree.add((byte) 0x1);
    else
      objectTree.add((byte) 0x0);
    // add space for the transitions
    for (int i = 0; i < numTransitions * (1 + indexBytes); i++) {
      objectTree.add((byte) 0x0);
    }
    int currentTransition = 0;
    while (tailValues != null && tailValues.size() > 0) {
      // find the transition byte for the first partition
      byte[] firstValue = tailValues.first();
      byte transitionByte = firstValue[indexDepth - 1];
      // calculate where to put the information for this transition edge
      int transitionLocation = entryPosition + 2 + ((1 + indexBytes) * currentTransition);
      // keep track of the byte on which we transition so that we can binary search for it later
      objectTree.set(transitionLocation, transitionByte);
      // determine whether there will be another partition, or if all remaining values have the same first transition from this state
      // this check is to avoid overflow when creating a partition point
      if (firstValue[indexDepth - 1] == (byte) 0xff) {
        // all remaining values have the same first transition
        headValues = tailValues;
        tailValues = null;
      } else {
        // there may be values that take another transition, so split the current value set
        // create a partition key that is just after all values with a transition of transitionByte
        byte[] head = new byte[indexDepth];
        for (int i = 0; i < indexDepth - 1; i++) {
          head[i] = firstValue[i];
        }
        head[indexDepth - 1] = (byte) ((firstValue[indexDepth - 1] & 0xff) + 1);
        // partition the remaining values based on the partition point
        headValues = tailValues.headSet(head);
        tailValues = tailValues.tailSet(head);
      }
      // process all the values with a transition of transitionByte
      // check to see if we can use the leaf state to save space
      if (headValues.size() == 1 && headValues.first().length == indexDepth) {
        // remove the value that transitions to the leaf state
        headValues.clear();
        // link to the leaf state
        objectTree.set(transitionLocation + 1, (byte) 0x0);
        objectTree.set(transitionLocation + 2, (byte) 0x0);
        objectTree.set(transitionLocation + 3, (byte) 0x0);
      } else {
        // link to a new state
        objectTree.set(transitionLocation + 1, (byte) (objectTree.size()));
        objectTree.set(transitionLocation + 2, (byte) (objectTree.size() >> 8));
        objectTree.set(transitionLocation + 3, (byte) (objectTree.size() >> 16));
        // create the new state
        create(headValues, indexDepth + 1);
      }
      // process the next transition
      currentTransition++;
    }
  }
  
  private void transitionState(byte val, State s) {
    if (s.failed)
      return;
    int i = 0;
    int j = s.size - 1;
    while (i <= j) {
      int k = (i + j) / 2;
      byte partKey = tree[s.start + ((1 + indexBytes) * k)];
      if (val > partKey) {
        i = k + 1;
      } else if (val < partKey) {
        j = k - 1;
      } else {
        int newStart = (tree[s.start + ((1 + indexBytes) * k) + 1] & 0xff) + ((tree[s.start + ((1 + indexBytes) * k) + 2] & 0xff) << 8)
            + ((tree[s.start + ((1 + indexBytes) * k) + 3] & 0xff) << 16);
        s.accept = (tree[newStart + 1] & 0xff) == 1;
        s.size = tree[newStart] & 0xff;
        s.start = newStart + 2;
        return;
      }
    }
    s.accept = false;
    s.failed = true;
  }
  
  State s = new State();
  
  public boolean check(byte[] value) {
    State s = new State();
    s.accept = tree[3] == (byte) 1;
    s.failed = false;
    s.size = tree[2] & 0xff;
    s.start = 4;
    for (int i = 0; i < value.length; i++) {
      transitionState(value[i], s);
    }
    return s.accept;
  }
  
  public void clearState() {
    s.accept = tree[3] == (byte) 1;
    s.failed = false;
    s.size = tree[2] & 0xff;
    s.start = 4;
  }
  
  public void transition(byte b) {
    transitionState(b, s);
  }
  
  public boolean check() {
    return s.failed == false && s.accept == true;
  }
  
}
