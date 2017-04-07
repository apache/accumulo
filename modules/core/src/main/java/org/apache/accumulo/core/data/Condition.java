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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.ColumnVisibility;
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

  /**
   * Creates a new condition. The initial column value and timestamp are null, and the initial column visibility is empty. Characters in the column family and
   * column qualifier are encoded as bytes in the condition using UTF-8.
   *
   * @param cf
   *          column family
   * @param cq
   *          column qualifier
   * @throws IllegalArgumentException
   *           if any argument is null
   */
  public Condition(CharSequence cf, CharSequence cq) {
    checkArgument(cf != null, "cf is null");
    checkArgument(cq != null, "cq is null");
    this.cf = new ArrayByteSequence(cf.toString().getBytes(UTF_8));
    this.cq = new ArrayByteSequence(cq.toString().getBytes(UTF_8));
    this.cv = EMPTY;
  }

  /**
   * Creates a new condition. The initial column value and timestamp are null, and the initial column visibility is empty.
   *
   * @param cf
   *          column family
   * @param cq
   *          column qualifier
   * @throws IllegalArgumentException
   *           if any argument is null
   */
  public Condition(byte[] cf, byte[] cq) {
    checkArgument(cf != null, "cf is null");
    checkArgument(cq != null, "cq is null");
    this.cf = new ArrayByteSequence(cf);
    this.cq = new ArrayByteSequence(cq);
    this.cv = EMPTY;
  }

  /**
   * Creates a new condition. The initial column value and timestamp are null, and the initial column visibility is empty.
   *
   * @param cf
   *          column family
   * @param cq
   *          column qualifier
   * @throws IllegalArgumentException
   *           if any argument is null
   */
  public Condition(Text cf, Text cq) {
    checkArgument(cf != null, "cf is null");
    checkArgument(cq != null, "cq is null");
    this.cf = new ArrayByteSequence(cf.getBytes(), 0, cf.getLength());
    this.cq = new ArrayByteSequence(cq.getBytes(), 0, cq.getLength());
    this.cv = EMPTY;
  }

  /**
   * Creates a new condition. The initial column value and timestamp are null, and the initial column visibility is empty.
   *
   * @param cf
   *          column family
   * @param cq
   *          column qualifier
   * @throws IllegalArgumentException
   *           if any argument is null
   */
  public Condition(ByteSequence cf, ByteSequence cq) {
    checkArgument(cf != null, "cf is null");
    checkArgument(cq != null, "cq is null");
    this.cf = cf;
    this.cq = cq;
    this.cv = EMPTY;
  }

  /**
   * Gets the column family of this condition.
   *
   * @return column family
   */
  public ByteSequence getFamily() {
    return cf;
  }

  /**
   * Gets the column qualifier of this condition.
   *
   * @return column qualifier
   */
  public ByteSequence getQualifier() {
    return cq;
  }

  /**
   * Sets the version for the column to check. If this is not set then the latest column will be checked, unless iterators do something different.
   *
   * @param ts
   *          timestamp
   * @return this condition
   */
  public Condition setTimestamp(long ts) {
    this.ts = ts;
    return this;
  }

  /**
   * Gets the timestamp of this condition.
   *
   * @return timestamp
   */
  public Long getTimestamp() {
    return ts;
  }

  /**
   * This method sets the expected value of a column. In order for the condition to pass the column must exist and have this value. If a value is not set, then
   * the column must be absent for the condition to pass. The passed-in character sequence is encoded as UTF-8. See {@link #setValue(byte[])}.
   *
   * @param value
   *          value
   * @return this condition
   * @throws IllegalArgumentException
   *           if value is null
   */
  public Condition setValue(CharSequence value) {
    checkArgument(value != null, "value is null");
    this.val = new ArrayByteSequence(value.toString().getBytes(UTF_8));
    return this;
  }

  /**
   * This method sets the expected value of a column. In order for the condition to pass the column must exist and have this value. If a value is not set, then
   * the column must be absent for the condition to pass.
   *
   * @param value
   *          value
   * @return this condition
   * @throws IllegalArgumentException
   *           if value is null
   */
  public Condition setValue(byte[] value) {
    checkArgument(value != null, "value is null");
    this.val = new ArrayByteSequence(value);
    return this;
  }

  /**
   * This method sets the expected value of a column. In order for the condition to pass the column must exist and have this value. If a value is not set, then
   * the column must be absent for the condition to pass. See {@link #setValue(byte[])}.
   *
   * @param value
   *          value
   * @return this condition
   * @throws IllegalArgumentException
   *           if value is null
   */
  public Condition setValue(Text value) {
    checkArgument(value != null, "value is null");
    this.val = new ArrayByteSequence(value.getBytes(), 0, value.getLength());
    return this;
  }

  /**
   * This method sets the expected value of a column. In order for the condition to pass the column must exist and have this value. If a value is not set, then
   * the column must be absent for the condition to pass. See {@link #setValue(byte[])}.
   *
   * @param value
   *          value
   * @return this condition
   * @throws IllegalArgumentException
   *           if value is null
   */
  public Condition setValue(ByteSequence value) {
    checkArgument(value != null, "value is null");
    this.val = value;
    return this;
  }

  /**
   * Gets the value of this condition.
   *
   * @return value
   */
  public ByteSequence getValue() {
    return val;
  }

  /**
   * Sets the visibility for the column to check. If not set it defaults to empty visibility.
   *
   * @param cv
   *          column visibility
   * @throws IllegalArgumentException
   *           if cv is null
   */
  public Condition setVisibility(ColumnVisibility cv) {
    checkArgument(cv != null, "cv is null");
    this.cv = new ArrayByteSequence(cv.getExpression());
    return this;
  }

  /**
   * Gets the column visibility of this condition.
   *
   * @return column visibility
   */
  public ByteSequence getVisibility() {
    return cv;
  }

  /**
   * Set iterators to use when reading the columns value. These iterators will be applied in addition to the iterators configured for the table. Using iterators
   * its possible to test other conditions, besides equality and absence, like less than. On the server side the iterators will be seeked using a range that
   * covers only the family, qualifier, and visibility (if the timestamp is set then it will be used to narrow the range). Value equality will be tested using
   * the first entry returned by the iterator stack.
   *
   * @param iterators
   *          iterators
   * @return this condition
   * @throws IllegalArgumentException
   *           if iterators or any of its elements are null, or if any two iterators share the same name or priority
   */
  public Condition setIterators(IteratorSetting... iterators) {
    checkArgument(iterators != null, "iterators is null");

    if (iterators.length > 1) {
      HashSet<String> names = new HashSet<>();
      HashSet<Integer> prios = new HashSet<>();

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

  /**
   * Gets the iterators for this condition.
   *
   * @return iterators
   */
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
