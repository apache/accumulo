/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 * A builder used to build {@link Key}s by defining their components.
 *
 * The rules are:
 * <ul>
 * <li>All components of the {@link Key} are optional except the row</li>
 * <li>Components not explicitly set default to empty byte array except the timestamp which defaults
 * to <code>Long.MAX_VALUE</code></li>
 * <li>The column qualifier can only be set if the column family has been set first</li>
 * <li>The column visibility can only be set if at least the column family has been set first</li>
 * </ul>
 *
 * The builder supports three types of components: <code>byte[]</code>, <code>Text</code> and
 * <code>CharSequence</code>. <code>CharSequence</code>s must be UTF-8 encoded.
 *
 * The builder is mutable and not thread safe.
 *
 * @see org.apache.accumulo.core.data.Key
 * @since 2.0
 */
public class KeyBuilder {

  /**
   * Base Builder interface which can be used to set the {@link Key} timestamp and delete marker and
   * to build the {@link Key}.
   *
   * @since 2.0
   */
  public interface Build {

    /**
     * Build a {@link Key} from this builder.
     *
     * @return the {@link Key} built from this builder
     */
    Key build();

    /**
     * Change the timestamp of the {@link Key} created.
     *
     * @param timestamp the timestamp to use for the {@link Key}
     * @return this builder
     */
    Build timestamp(long timestamp);

    /**
     * Set the deleted marker of the {@link Key} to the parameter.
     *
     * @param deleted if the {@link Key} should be marked as deleted or not
     * @return this builder
     */
    Build deleted(boolean deleted);
  }

  /**
   * Builder step used to set the row part of the {@link Key}.
   *
   * @since 2.0
   */
  public interface RowStep extends Build {

    /**
     * Set the row of the {@link Key} that this builder will build to the parameter.
     *
     * @param row the row to use for the key
     * @return this builder
     */
    ColumnFamilyStep row(final Text row);

    /**
     * Set the row of the {@link Key} that this builder will build to the parameter.
     *
     * @param row the row to use for the key
     * @return this builder
     */
    ColumnFamilyStep row(final byte[] row);

    /**
     * Set the row of the {@link Key} that this builder will build to the parameter.
     *
     * @param row the row to use for the key
     * @param offset the offset within the array of the first byte to be read; must be non-negative
     *        and no larger than row.length
     * @param length the number of bytes to be read from the given array; must be non-negative and
     *        no larger than row.length - offset
     * @return this builder
     */
    ColumnFamilyStep row(final byte[] row, int offset, int length);

    /**
     * Set the row of the {@link Key} that this builder will build to the parameter.
     *
     * @param row the row to use for the key. The encoding must be UTF-8
     * @return this builder
     */
    ColumnFamilyStep row(final CharSequence row);
  }

  /**
   * Builder step used to set the columnFamily part of the {@link Key}.
   *
   * @since 2.0
   */
  public interface ColumnFamilyStep extends ColumnVisibilityStep {

    /**
     * Set the column family of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnFamily the column family to use for the {@link Key}
     * @return this builder
     */
    ColumnQualifierStep family(final byte[] columnFamily);

    /**
     * Set the column family of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnFamily the column family to use for the {@link Key}
     * @param offset the offset within the array of the first byte to be read; must be non-negative
     *        and no larger than row.length
     * @param length the number of bytes to be read from the given array; must be non-negative and
     *        no larger than row.length - offset
     * @return this builder
     */
    ColumnQualifierStep family(final byte[] columnFamily, int offset, int length);

    /**
     * Set the column family of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnFamily the column family to use for the {@link Key}
     * @return this builder
     */
    ColumnQualifierStep family(final Text columnFamily);

    /**
     * Set the column family of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnFamily the column family to use for the {@link Key}. The encoding must be UTF-8
     * @return this builder
     */
    ColumnQualifierStep family(final CharSequence columnFamily);
  }

  /**
   * Builder step used to set the column qualifier part of the {@link Key}.
   *
   * @since 2.0
   */
  public interface ColumnQualifierStep extends ColumnVisibilityStep {

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnQualifier the column qualifier to use for the {@link Key}
     * @return this builder
     */
    ColumnVisibilityStep qualifier(final byte[] columnQualifier);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnQualifier the column qualifier to use for the {@link Key}
     * @param offset the offset within the array of the first byte to be read; must be non-negative
     *        and no larger than row.length
     * @param length the number of bytes to be read from the given array; must be non-negative and
     *        no larger than row.length - offset
     * @return this builder
     */
    ColumnVisibilityStep qualifier(final byte[] columnQualifier, int offset, int length);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnQualifier the column qualifier to use for the {@link Key}
     * @return this builder
     */
    ColumnVisibilityStep qualifier(final Text columnQualifier);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnQualifier the column qualifier to use for the {@link Key}. The encoding must be
     *        UTF-8
     * @return this builder
     */
    ColumnVisibilityStep qualifier(final CharSequence columnQualifier);
  }

  /**
   * Builder step used to set the column visibility part of the {@link Key}.
   *
   * @since 2.0
   */
  public interface ColumnVisibilityStep extends Build {

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility the column visibility to use for the {@link Key}
     * @return this builder
     */
    Build visibility(final byte[] columnVisibility);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility the column visibility to use for the {@link Key}
     * @param offset the offset within the array of the first byte to be read; must be non-negative
     *        and no larger than row.length
     * @param length the number of bytes to be read from the given array; must be non-negative and
     *        no larger than row.length - offset
     * @return this builder
     */
    Build visibility(final byte[] columnVisibility, int offset, int length);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility the column visibility to use for the {@link Key}
     * @return this builder
     */
    Build visibility(final Text columnVisibility);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility the column visibility to use for the {@link Key}. The encoding must
     *        be UTF-8
     * @return this builder
     */
    Build visibility(final CharSequence columnVisibility);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility the column visibility to use for the {@link Key}
     * @return this builder
     */
    Build visibility(final ColumnVisibility columnVisibility);
  }

  /**
   * @since 2.0
   */
  static class KeyBuilderImpl
      implements RowStep, ColumnFamilyStep, ColumnQualifierStep, ColumnVisibilityStep {

    protected static final byte[] EMPTY_BYTES = new byte[0];

    private final boolean copyBytes;
    private byte[] row = EMPTY_BYTES;
    private int rowOffset = 0;
    private int rowLength = 0;
    private byte[] family = EMPTY_BYTES;
    private int familyOffset = 0;
    private int familyLength = 0;
    private byte[] qualifier = EMPTY_BYTES;
    private int qualifierOffset = 0;
    private int qualifierLength = 0;
    private byte[] visibility = EMPTY_BYTES;
    private int visibilityOffset = 0;
    private int visibilityLength = 0;
    private long timestamp = Long.MAX_VALUE;
    private boolean deleted = false;

    KeyBuilderImpl(boolean copyBytes) {
      this.copyBytes = copyBytes;
    }

    private byte[] copyBytesIfNeeded(final byte[] bytes, int offset, int length) {
      return Key.copyIfNeeded(bytes, offset, length, this.copyBytes);
    }

    private byte[] encodeCharSequence(CharSequence chars) {
      return chars.toString().getBytes(UTF_8);
    }

    @Override
    public ColumnFamilyStep row(final byte[] row, int offset, int length) {
      this.row = copyBytesIfNeeded(row, offset, length);
      this.rowOffset = this.copyBytes ? 0 : offset;
      this.rowLength = this.copyBytes ? this.row.length : length;
      return this;
    }

    @Override
    public ColumnFamilyStep row(final byte[] row) {
      return row(row, 0, row.length);
    }

    @Override
    public ColumnFamilyStep row(final Text row) {
      return row(row.getBytes(), 0, row.getLength());
    }

    @Override
    public ColumnFamilyStep row(final CharSequence row) {
      return row(encodeCharSequence(row));
    }

    @Override
    public ColumnQualifierStep family(final byte[] family, int offset, int length) {
      this.family = copyBytesIfNeeded(family, offset, length);
      this.familyOffset = this.copyBytes ? 0 : offset;
      this.familyLength = this.copyBytes ? this.family.length : length;
      return this;
    }

    @Override
    public ColumnQualifierStep family(final byte[] family) {
      return family(family, 0, family.length);
    }

    @Override
    public ColumnQualifierStep family(Text family) {
      return family(family.getBytes(), 0, family.getLength());
    }

    @Override
    public ColumnQualifierStep family(CharSequence family) {
      return family(encodeCharSequence(family));
    }

    @Override
    public ColumnVisibilityStep qualifier(final byte[] qualifier, int offset, int length) {
      this.qualifier = copyBytesIfNeeded(qualifier, offset, length);
      this.qualifierOffset = this.copyBytes ? 0 : offset;
      this.qualifierLength = this.copyBytes ? this.qualifier.length : length;
      return this;
    }

    @Override
    public ColumnVisibilityStep qualifier(final byte[] qualifier) {
      return qualifier(qualifier, 0, qualifier.length);
    }

    @Override
    public ColumnVisibilityStep qualifier(Text qualifier) {
      return qualifier(qualifier.getBytes(), 0, qualifier.getLength());
    }

    @Override
    public ColumnVisibilityStep qualifier(CharSequence qualifier) {
      return qualifier(encodeCharSequence(qualifier));
    }

    @Override
    public Build visibility(final byte[] visibility, int offset, int length) {
      this.visibility = copyBytesIfNeeded(visibility, offset, length);
      this.visibilityOffset = this.copyBytes ? 0 : offset;
      this.visibilityLength = this.copyBytes ? this.visibility.length : length;
      return this;
    }

    @Override
    public Build visibility(final byte[] visibility) {
      return visibility(visibility, 0, visibility.length);
    }

    @Override
    public Build visibility(Text visibility) {
      return visibility(visibility.getBytes(), 0, visibility.getLength());
    }

    @Override
    public Build visibility(CharSequence visibility) {
      return visibility(encodeCharSequence(visibility));
    }

    @Override
    public Build visibility(ColumnVisibility visibility) {
      byte[] expr = visibility.getExpression();
      return visibility(expr, 0, expr.length);
    }

    @Override
    public final Build timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override
    public Build deleted(boolean deleted) {
      this.deleted = deleted;
      return this;
    }

    @Override
    public Key build() {
      return new Key(this.row, this.rowOffset, this.rowLength, this.family, this.familyOffset,
          this.familyLength, this.qualifier, this.qualifierOffset, this.qualifierLength,
          this.visibility, this.visibilityOffset, this.visibilityLength, this.timestamp,
          this.deleted, false);
    }
  }
}
