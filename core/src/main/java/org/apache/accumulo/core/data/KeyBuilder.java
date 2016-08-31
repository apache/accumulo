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


import org.apache.hadoop.io.Text;

/**
 * A builder used to build <code>Key</code>s by defining their components.
 *
 * The rules are:
 * <ul>
 *   <li>All components of the <code>Key</code> are optional except the row</li>
 *   <li>Components not explicitly set default to empty byte array except the timestamp which
 *   defaults to <code>Long.MAX_VALUE</code></li>
 *   <li>The column qualifier can only be set if the column family has been set first</li>
 *   <li>The column visibility can only be set if at least the column family has been set first</li>
 * </ul>
 *
 * The builder supports three types of components: <code>byte[]</code>, <code>Text</code> and <code>CharSequence</code>.
 *
 * The builder is mutable and not thread safe.
 *
 * @see org.apache.accumulo.core.data.Key
 * @since 1.8
 */
public class KeyBuilder {

  public interface Build<T> {

    /**
     * Build a <code>Key</code> from this builder.
     *
     * @param copyBytes
     *          if the byte arrays should be copied or not. If not, byte arrays will be reused in the resultant <code>Key</code>
     * @return
     *          the <code>Key</code> built from this builder
     */
    Key build(boolean copyBytes);

    /**
     * Build a <code>Key</code> from this builder. copyBytes defaults to true.
     *
     * @return
     *          the <code>Key</code> built from this builder
     */
    Key build();

    /**
     * Change the timestamp of the <code>Key</code> created.
     *
     * @param timestamp
     *          the timestamp to use for the <code>Key</code>
     * @return this builder
     */
    Build<T> timestamp(long timestamp);

    /**
     * Set the deleted marker of the <code>Key</code> to the parameter.
     *
     * @param deleted
     *          if the <code>Key</code> should be marked as deleted or not
     * @return this builder
     */
    Build<T> deleted(boolean deleted);
  }

  public interface ColumnFamilyStep<T> extends ColumnVisibilityStep<T> {

    /**
     * Set the column family of the <code>Key</code> that this builder will build to the parameter.
     *
     * @param columnFamily
     *          the column family to use for the <code>Key</code>
     * @return this builder
     */
    ColumnQualifierStep<T> columnFamily(final T columnFamily);

    /**
     * Set the column family, qualifier and visibility of the <code>Key</code> that this builder will build to the parameter.
     *
     * @param columnFamily
     *          the column family to use for the <code>Key</code>
     * @param columnQualifier
     *          the column qualifier to use for the <code>Key</code>
     * @param columnVisibility
     *          the column visibility to use for the <code>Key</code>
     * @return this builder
     */
    Build<T> column(final T columnFamily, final T columnQualifier, final T columnVisibility);

    /**
     * Set the column family and the qualifier of the <code>Key</code> that this builder will build to the parameter.
     *
     * @param columnFamily
     *          the column family to use for the <code>Key</code>
     * @param columnQualifier
     *          the column qualifier to use for the <code>Key</code>
     * @return this builder
     */
    ColumnVisibilityStep<T> column(final T columnFamily, final T columnQualifier);
  }

  public interface ColumnQualifierStep<T> extends ColumnVisibilityStep<T> {

    /**
     * Set the column qualifier of the <code>Key</code> that this builder will build to the parameter.
     *
     * @param columnQualifier
     *          the column qualifier to use for the <code>Key</code>
     * @return this builder
     */
    ColumnVisibilityStep<T> columnQualifier(final T columnQualifier);
  }

  public interface ColumnVisibilityStep<T> extends Build<T> {
    /**
     * Set the column qualifier of the <code>Key</code> that this builder will build to the parameter.
     *
     * @param columnVisibility
     *          the column visibility to use for the <code>Key</code>
     * @return this builder
     */
    Build<T> columnVisibility(final T columnVisibility);
  }

  private static abstract class AbstractKeyBuilder<T> implements ColumnFamilyStep<T>, ColumnQualifierStep<T>,
      ColumnVisibilityStep<T> {

    protected static final byte EMPTY_BYTES[] = new byte[0];

    protected T row = null;
    protected T columnFamily = null;
    protected T columnQualifier = null;
    protected T columnVisibility = null;
    protected long timestamp = Long.MAX_VALUE;
    protected boolean deleted = false;

    final public ColumnFamilyStep<T> row(final T row) {
      this.row = row;
      return this;
    }

    @Override
    final public ColumnQualifierStep<T> columnFamily(final T columnFamily) {
      this.columnFamily = columnFamily;
      return this;
    }

    @Override
    final public ColumnVisibilityStep<T> columnQualifier(final T columnQualifier) {
      this.columnQualifier = columnQualifier;
      return this;
    }

    @Override
    final public Build<T> columnVisibility(final T columnVisibility) {
      this.columnVisibility = columnVisibility;
      return this;
    }

    @Override
    final public Build<T> timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override
    public Build<T> deleted(boolean deleted) {
      this.deleted = deleted;
      return this;
    }

    @Override
    public Key build() {
      return this.build(true);
    }

    @Override
    public Build<T> column(final T columnFamily, final T columnQualifier, final T columnVisibility) {
      return this.columnFamily(columnFamily).columnQualifier(columnQualifier).columnVisibility(columnVisibility);
    }

    @Override
    public ColumnVisibilityStep<T> column(final T columnFamily, final T columnQualifier) {
      return this.columnFamily(columnFamily).columnQualifier(columnQualifier);
    }
  }

  private static class TextKeyBuilder extends AbstractKeyBuilder<Text> {

    private final Text EMPTY_TEXT = new Text();

    @Override
    public Key build(boolean copyBytes) {
      Text columnFamily = (this.columnFamily == null) ? EMPTY_TEXT : this.columnFamily;
      Text columnQualifier = (this.columnQualifier == null) ? EMPTY_TEXT : this.columnQualifier;
      Text columnVisibility = (this.columnVisibility == null) ? EMPTY_TEXT : this.columnVisibility;
      return new Key(row.getBytes(), 0, row.getLength(), columnFamily.getBytes(), 0, columnFamily.getLength(),
          columnQualifier.getBytes(), 0, columnQualifier.getLength(), columnVisibility.getBytes(), 0,
          columnVisibility.getLength(), timestamp, deleted, copyBytes);
    }
  }

  private static class ByteArrayKeyBuilder extends AbstractKeyBuilder<byte[]> {

    @Override
    public Key build(boolean copyBytes) {
      byte[] columnFamily = (this.columnFamily == null) ? EMPTY_BYTES : this.columnFamily;
      byte[] columnQualifier = (this.columnQualifier == null) ? EMPTY_BYTES : this.columnQualifier;
      byte[] columnVisibility = (this.columnVisibility == null) ? EMPTY_BYTES : this.columnVisibility;
      return new Key(row, columnFamily, columnQualifier, columnVisibility, timestamp, deleted, copyBytes);
    }
  }

  private static class CharSequenceKeyBuilder extends AbstractKeyBuilder<CharSequence> {

    private final Text EMPTY_TEXT = new Text();

    @Override
    public Key build(boolean copyBytes) {
      Text rowText = new Text(row.toString());
      Text columnFamilyText = (this.columnFamily == null) ? EMPTY_TEXT : new Text(this.columnFamily.toString());
      Text columnQualifierText = (this.columnQualifier == null) ? EMPTY_TEXT : new Text(this.columnQualifier.toString());
      Text columnVisibilityText = (this.columnVisibility == null) ? EMPTY_TEXT : new Text(this.columnVisibility.toString());
      return new Key(rowText.getBytes(), 0, rowText.getLength(), columnFamilyText.getBytes(), 0,
          columnFamilyText.getLength(), columnQualifierText.getBytes(), 0, columnQualifierText.getLength(),
          columnVisibilityText.getBytes(), 0, columnVisibilityText.getLength(), timestamp, deleted, copyBytes);
    }
  }

  /**
   * Set the row of the <code>Key</code> that this builder will build to the parameter.
   *
   * @param row
   *          the row to use for the key
   * @return this builder
   */
  public static ColumnFamilyStep<Text> row(final Text row) {
    return new TextKeyBuilder().row(row);
  }

  /**
   * Set the row of the <code>Key</code> that this builder will build to the parameter.
   *
   * @param row
   *          the row to use for the key
   * @return this builder
   */
  public static ColumnFamilyStep<byte[]> row(final byte[] row) {
    return new ByteArrayKeyBuilder().row(row);
  }

  /**
   * Set the row of the <code>Key</code> that this builder will build to the parameter.
   *
   * @param row
   *          the row to use for the key
   * @return this builder
   */
  public static ColumnFamilyStep<CharSequence> row( final CharSequence row) {
    return new CharSequenceKeyBuilder().row(row);
  }
}
