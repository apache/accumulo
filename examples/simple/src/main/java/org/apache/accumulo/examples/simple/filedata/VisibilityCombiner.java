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
package org.apache.accumulo.examples.simple.filedata;

import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;

/**
 * A utility for merging visibilities into the form {@code (VIS1)|(VIS2)|...|(VISN)}. Used by the {@link ChunkCombiner}.
 */
public class VisibilityCombiner {

  private TreeSet<String> visibilities = new TreeSet<String>();

  void add(ByteSequence cv) {
    if (cv.length() == 0)
      return;

    int depth = 0;
    int offset = 0;

    for (int i = 0; i < cv.length(); i++) {
      switch (cv.byteAt(i)) {
        case '(':
          depth++;
          break;
        case ')':
          depth--;
          if (depth < 0)
            throw new IllegalArgumentException("Invalid vis " + cv);
          break;
        case '|':
          if (depth == 0) {
            insert(cv.subSequence(offset, i));
            offset = i + 1;
          }

          break;
      }
    }

    insert(cv.subSequence(offset, cv.length()));

    if (depth != 0)
      throw new IllegalArgumentException("Invalid vis " + cv);

  }

  private void insert(ByteSequence cv) {
    for (int i = 0; i < cv.length(); i++) {

    }

    String cvs = cv.toString();

    if (cvs.charAt(0) != '(')
      cvs = "(" + cvs + ")";
    else {
      int depth = 0;
      int depthZeroCloses = 0;
      for (int i = 0; i < cv.length(); i++) {
        switch (cv.byteAt(i)) {
          case '(':
            depth++;
            break;
          case ')':
            depth--;
            if (depth == 0)
              depthZeroCloses++;
            break;
        }
      }

      if (depthZeroCloses > 1)
        cvs = "(" + cvs + ")";
    }

    visibilities.add(cvs);
  }

  byte[] get() {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (String cvs : visibilities) {
      sb.append(sep);
      sep = "|";
      sb.append(cvs);
    }

    return sb.toString().getBytes();
  }
}
