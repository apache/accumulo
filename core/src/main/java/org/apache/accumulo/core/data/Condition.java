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
package org.apache.accumulo.core.data;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

/**
 * Conditions that must be met on a particular column in a row.
 *
 * @since 1.6.0
 */
public class Condition {

  private ByteSequence cf;
  private ByteSequence cq;
  private ByteSequence cv;
  private ByteSequence val;
  private Long ts;
  private IteratorSetting iterators[] = new IteratorSetting[0];
  private static final ByteSequence EMPTY = new ArrayByteSequence(new byte[0]);

  public Condition(CharSequence cf, CharSequence cq) {
    ArgumentChecker.notNull(cf, cq);
    this.cf = new ArrayByteSequence(cf.toString().getBytes(UTF_8));
    this.cq = new ArrayByteSequence(cq.toString().getBytes(UTF_8));
    this.cv = EMPTY;
  }

  public Condition(byte[] cf, byte[] cq) {
    ArgumentChecker.notNull(cf, cq);
    this.cf = new ArrayByteSequence(cf);
    this.cq = new ArrayByteSequence(cq);
    this.cv = EMPTY;
  }

  public Condition(Text cf, Text cq) {
    ArgumentChecker.notNull(cf, cq);
    this.cf = new ArrayByteSequence(cf.getBytes(), 0, cf.getLength());
    this.cq = new ArrayByteSequence(cq.getBytes(), 0, cq.getLength());
    this.cv = EMPTY;
  }

  public Condition(ByteSequence cf, ByteSequence cq) {
    ArgumentChecker.notNull(cf, cq);
    this.cf = cf;
    this.cq = cq;
    this.cv = EMPTY;
  }

  public ByteSequence getFamily() {
    return cf;
  }

  public ByteSequence getQualifier() {
    return cq;
  }

  /**
   * Sets the version for the column to check. If this is not set then the latest column will be checked, unless iterators do something different.
   *
   * @return returns this
   */

  public Condition setTimestamp(long ts) {
    this.ts = ts;
    return this;
  }

  public Long getTimestamp() {
    return ts;
  }

  /**
   * see {@link #setValue(byte[])}
   *
   * @return returns this
   */

  public Condition setValue(CharSequence value) {
    ArgumentChecker.notNull(value);
    this.val = new ArrayByteSequence(value.toString().getBytes(UTF_8));
    return this;
  }

  /**
   * This method sets the expected value of a column. Inorder for the condition to pass the column must exist and have this value. If a value is not set, then
   * the column must be absent for the condition to pass.
   *
   * @return returns this
   */

  public Condition setValue(byte[] value) {
    ArgumentChecker.notNull(value);
    this.val = new ArrayByteSequence(value);
    return this;
  }

  /**
   * see {@link #setValue(byte[])}
   *
   * @return returns this
   */

  public Condition setValue(Text value) {
    ArgumentChecker.notNull(value);
    this.val = new ArrayByteSequence(value.getBytes(), 0, value.getLength());
    return this;
  }

  /**
   * see {@link #setValue(byte[])}
   *
   * @return returns this
   */

  public Condition setValue(ByteSequence value) {
    ArgumentChecker.notNull(value);
    this.val = value;
    return this;
  }

  public ByteSequence getValue() {
    return val;
  }

  /**
   * Sets the visibility for the column to check. If not set it defaults to empty visibility.
   *
   * @return returns this
   */

  public Condition setVisibility(ColumnVisibility cv) {
    ArgumentChecker.notNull(cv);
    this.cv = new ArrayByteSequence(cv.getExpression());
    return this;
  }

  public ByteSequence getVisibility() {
    return cv;
  }

  /**
   * Set iterators to use when reading the columns value. These iterators will be applied in addition to the iterators configured for the table. Using iterators
   * its possible to test other conditions, besides equality and absence, like less than. On the server side the iterators will be seeked using a range that
   * covers only the family, qualifier, and visibility (if the timestamp is set then it will be used to narrow the range). Value equality will be tested using
   * the first entry returned by the iterator stack.
   *
   * @return returns this
   */

  public Condition setIterators(IteratorSetting... iterators) {
    ArgumentChecker.notNull(iterators);

    if (iterators.length > 1) {
      HashSet<String> names = new HashSet<String>();
      HashSet<Integer> prios = new HashSet<Integer>();

      for (IteratorSetting iteratorSetting : iterators) {
        if (!names.add(iteratorSetting.getName()))
          throw new IllegalArgumentException("iterator name used more than once " + iteratorSetting.getName());
        if (!prios.add(iteratorSetting.getPriority()))
          throw new IllegalArgumentException("iterator priority used more than once " + iteratorSetting.getPriority());
      }
    }

    this.iterators = iterators;
    return this;
  }

  public IteratorSetting[] getIterators() {
    return iterators;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof Condition)) {
      return false;
    }
    Condition c = (Condition) o;
    if (!(c.cf.equals(cf))) {
      return false;
    }
    if (!(c.cq.equals(cq))) {
      return false;
    }
    if (!(c.cv.equals(cv))) {
      return false;
    }
    if (!(c.val == null ? val == null : c.val.equals(val))) {
      return false;
    }
    if (!(c.ts == null ? ts == null : c.ts.equals(ts))) {
      return false;
    }
    if (!(Arrays.equals(c.iterators, iterators))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + cf.hashCode();
    result = 31 * result + cq.hashCode();
    result = 31 * result + cv.hashCode();
    result = 31 * result + (val == null ? 0 : val.hashCode());
    result = 31 * result + (ts == null ? 0 : ts.hashCode());
    result = 31 * result + Arrays.hashCode(iterators);
    return result;
  }

}
