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
 * A builder used to build {@link Key}s by defining their components.
 *
 * The rules are:
 * <ul>
 *   <li>All components of the {@link Key} are optional except the row</li>
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

  public interface Build {

    /**
     * Build a {@link Key} from this builder. copyBytes defaults to true.
     *
     * @return
     *          the {@link Key} built from this builder
     */
    Key build();

    /**
     * Change the timestamp of the {@link Key} created.
     *
     * @param timestamp
     *          the timestamp to use for the {@link Key}
     * @return this builder
     */
    Build timestamp(long timestamp);

    /**
     * Set the deleted marker of the {@link Key} to the parameter.
     *
     * @param deleted
     *          if the {@link Key} should be marked as deleted or not
     * @return this builder
     */
    Build deleted(boolean deleted);
  }

  public interface RowStep extends Build {

    /**
     * Set the row of the {@link Key} that this builder will build to the parameter.
     *
     * @param row
     *          the row to use for the key
     * @return this builder
     */
    ColumnFamilyStep row(final Text row);

    /**
     * Set the row of the {@link Key} that this builder will build to the parameter.
     *
     * @param row
     *          the row to use for the key
     * @return this builder
     */
    ColumnFamilyStep row(final byte[] row);

    /**
     * Set the row of the {@link Key} that this builder will build to the parameter.
     *
     * @param row
     *          the row to use for the key
     * @return this builder
     */
    ColumnFamilyStep row(final CharSequence row);
  }

  public interface ColumnFamilyStep extends ColumnVisibilityStep {

    /**
     * Set the column family of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnFamily
     *          the column family to use for the {@link Key}
     * @return this builder
     */
    ColumnQualifierStep family(final byte[] columnFamily);

    /**
     * Set the column family of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnFamily
     *          the column family to use for the {@link Key}
     * @return this builder
     */
    ColumnQualifierStep family(final Text columnFamily);

    /**
     * Set the column family of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnFamily
     *          the column family to use for the {@link Key}
     * @return this builder
     */
    ColumnQualifierStep family(final CharSequence columnFamily);
  }

  public interface ColumnQualifierStep extends ColumnVisibilityStep {

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnQualifier
     *          the column qualifier to use for the {@link Key}
     * @return this builder
     */
    ColumnVisibilityStep qualifier(final byte[] columnQualifier);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnQualifier
     *          the column qualifier to use for the {@link Key}
     * @return this builder
     */
    ColumnVisibilityStep qualifier(final Text columnQualifier);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnQualifier
     *          the column qualifier to use for the {@link Key}
     * @return this builder
     */
    ColumnVisibilityStep qualifier(final CharSequence columnQualifier);
  }

  public interface ColumnVisibilityStep extends Build {

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility
     *          the column visibility to use for the {@link Key}
     * @return this builder
     */
    Build visibility(final byte[] columnVisibility);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility
     *          the column visibility to use for the {@link Key}
     * @return this builder
     */
    Build visibility(final Text columnVisibility);

    /**
     * Set the column qualifier of the {@link Key} that this builder will build to the parameter.
     *
     * @param columnVisibility
     *          the column visibility to use for the {@link Key}
     * @return this builder
     */
    Build visibility(final CharSequence columnVisibility);
  }

  static class AbstractKeyBuilder implements RowStep, ColumnFamilyStep, ColumnQualifierStep,
      ColumnVisibilityStep {

    protected static final byte EMPTY_BYTES[] = new byte[0];

    private final boolean copyBytes;
    private byte[] row = EMPTY_BYTES;
    private byte[] family = EMPTY_BYTES;
    private byte[] qualifier = EMPTY_BYTES;
    private byte[] visibility = EMPTY_BYTES;
    private long timestamp = Long.MAX_VALUE;
    private boolean deleted = false;

    AbstractKeyBuilder(boolean copyBytes) {
      this.copyBytes = copyBytes;
    }

    private byte[] copyBytesIfNeeded(final byte[] bytes) {
      return Key.copyIfNeeded(bytes, 0, bytes.length, this.copyBytes);
    }

    private byte[] copyBytesIfNeeded(Text text) {
      return Key.copyIfNeeded(text.getBytes(), 0, text.getLength(), this.copyBytes);
    }

    public ColumnFamilyStep row(final byte[] row) {
      this.row = copyBytesIfNeeded(row);
      return this;
    }

    public ColumnFamilyStep row(final Text row) {
      this.row = copyBytesIfNeeded(row);
      return this;
    }

    public ColumnFamilyStep row(final CharSequence row) {
      return row(new Text(row.toString()));
    }

    @Override
    public ColumnQualifierStep family(final byte[] family) {
      this.family = copyBytesIfNeeded(family);
      return this;
    }


    @Override
    public ColumnQualifierStep family(Text family) {
      this.family = copyBytesIfNeeded(family);
      return this;
    }

    @Override
    public ColumnQualifierStep family(CharSequence family) {
      return family(new Text(family.toString()));
    }

    @Override
    public ColumnVisibilityStep qualifier(byte[] qualifier) {
      this.qualifier = copyBytesIfNeeded(qualifier);
      return this;
    }

    @Override
    public ColumnVisibilityStep qualifier(Text qualifier) {
      this.qualifier = copyBytesIfNeeded(qualifier);
      return this;
    }

    @Override
    public ColumnVisibilityStep qualifier(CharSequence qualifier) {
      return qualifier(new Text(qualifier.toString()));
    }

    @Override
    public Build visibility(byte[] visibility) {
      this.visibility = copyBytesIfNeeded(visibility);
      return this;
    }

    @Override
    public Build visibility(Text visibility) {
      this.visibility = copyBytesIfNeeded(visibility);
      return this;
    }

    @Override
    public Build visibility(CharSequence visibility) {
      return visibility(new Text(visibility.toString()));
    }

    @Override
    final public Build timestamp(long timestamp) {
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
      return new Key(this.row, this.family, this.qualifier, this.visibility, this.timestamp, this.deleted, false);
    }
  }
}
