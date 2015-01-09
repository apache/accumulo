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
package org.apache.accumulo.core.util;

import static com.google.common.base.Charsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

public class ByteArraySet extends TreeSet<byte[]> {

  private static final long serialVersionUID = 1L;

  public ByteArraySet() {
    super(new ByteArrayComparator());
  }

  public ByteArraySet(Collection<? extends byte[]> c) {
    this();
    addAll(c);
  }

  public static ByteArraySet fromStrings(Collection<String> c) {
    List<byte[]> lst = new ArrayList<byte[]>();
    for (String s : c)
      lst.add(s.getBytes(UTF_8));
    return new ByteArraySet(lst);
  }

  public static ByteArraySet fromStrings(String... c) {
    return ByteArraySet.fromStrings(Arrays.asList(c));
  }

  public List<byte[]> toList() {
    return new ArrayList<byte[]>(this);
  }

}
