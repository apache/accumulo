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

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.admin.TableOperations.ImportMappingOptions;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedBytes;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Information about where to load files into an Accumulo table.
 *
 * @see ImportMappingOptions#plan(LoadPlan)
 * @since 2.0.0
 */
public class LoadPlan {
  private final List<Destination> destinations;

  private static byte[] copy(byte[] data) {
    return data == null ? null : Arrays.copyOf(data, data.length);
  }

  private static byte[] copy(Text data) {
    return data == null ? null : data.copyBytes();
  }

  private static byte[] copy(CharSequence data) {
    return data == null ? null : data.toString().getBytes(UTF_8);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "this code is validating the input")
  private static String checkFileName(String fileName) {
    Preconditions.checkArgument(Paths.get(fileName).getNameCount() == 1,
        "Expected only filename, but got %s", fileName);
    return fileName;
  }

  /**
   * @since 2.0.0
   */
  public enum RangeType {
    /**
     * Range that corresponds to one or more tablets in a table. For a range of this type, the start
     * row and end row can be null. The start row is exclusive and the end row is inclusive (like
     * Accumulo tablets). A common use case for this would be when files were partitioned using a
     * table's splits. When using this range type, the start and end row must exist as splits in the
     * table or an exception will be thrown at load time.
     */
    TABLE,
    /**
     * Range that correspond to known rows in a file. For this range type, the start row and end row
     * must be non-null. The start row and end row are both considered inclusive. At load time,
     * these data ranges will be mapped to table ranges.
     */
    FILE
  }

  /**
   * Mapping of a file to a row range with an associated range type.
   *
   * @since 2.0.0
   */
  public static class Destination {

    private final String fileName;
    private final byte[] startRow;
    private final byte[] endRow;
    private final RangeType rangeType;

    private byte[] checkRow(RangeType type, byte[] row) {
      if (type == RangeType.FILE && row == null) {
        throw new IllegalArgumentException(
            "Row can not be null when range type is " + RangeType.FILE);
      }
      return row;
    }

    private Destination(String fileName, RangeType rangeType, byte[] startRow, byte[] endRow) {
      this.fileName = checkFileName(fileName);
      this.rangeType = rangeType;
      this.startRow = checkRow(rangeType, startRow);
      this.endRow = checkRow(rangeType, endRow);

      if (rangeType == RangeType.FILE) {
        if (UnsignedBytes.lexicographicalComparator().compare(startRow, endRow) > 0) {
          String srs = new String(startRow, UTF_8);
          String ers = new String(endRow, UTF_8);
          throw new IllegalArgumentException(
              "Start row is greater than end row : " + srs + " " + ers);
        }
      } else if (rangeType == RangeType.TABLE) {
        if (startRow != null && endRow != null
            && UnsignedBytes.lexicographicalComparator().compare(startRow, endRow) >= 0) {
          String srs = new String(startRow, UTF_8);
          String ers = new String(endRow, UTF_8);
          throw new IllegalArgumentException(
              "Start row is greater than or equal to end row : " + srs + " " + ers);
        }
      } else {
        throw new RuntimeException();
      }

    }

    public String getFileName() {
      return fileName;
    }

    public byte[] getStartRow() {
      return copy(startRow);
    }

    public byte[] getEndRow() {
      return copy(endRow);
    }

    public RangeType getRangeType() {
      return rangeType;
    }
  }

  private LoadPlan(List<Destination> destinations) {
    this.destinations = destinations;
  }

  public Collection<Destination> getDestinations() {
    return destinations;
  }

  /**
   * @since 2.0.0
   */
  public interface Builder {
    /**
     * Specify the row range where a file should be loaded. Note that whether the startRow parameter
     * is inclusive or exclusive is determined by the {@link RangeType} parameter.
     *
     * @param fileName this should not be a path. Only a file name because loads are expected to
     *        happen from a single directory.
     */
    Builder loadFileTo(String fileName, RangeType rangeType, Text startRow, Text endRow);

    /**
     * Specify the row range where a file should be loaded. Note that whether the startRow parameter
     * is inclusive or exclusive is determined by the {@link RangeType} parameter.
     *
     * @param fileName this should not be a path. Only a file name because loads are expected to
     *        happen from a single directory.
     */
    Builder loadFileTo(String fileName, RangeType rangeType, byte[] startRow, byte[] endRow);

    /**
     * Specify the row range where a file should be loaded. Note that whether the startRow parameter
     * is inclusive or exclusive is determined by the {@link RangeType} parameter.
     *
     * @param fileName this should not be a path. Only a file name because loads are expected to
     *        happen from a single directory.
     */
    Builder loadFileTo(String fileName, RangeType rangeType, CharSequence startRow,
        CharSequence endRow);

    Builder addPlan(LoadPlan plan);

    LoadPlan build();
  }

  public static Builder builder() {
    return new Builder() {
      final ImmutableList.Builder<Destination> fmb = ImmutableList.builder();

      @Override
      public Builder loadFileTo(String fileName, RangeType rangeType, Text startRow, Text endRow) {
        fmb.add(new Destination(fileName, rangeType, copy(startRow), copy(endRow)));
        return this;
      }

      @Override
      public Builder loadFileTo(String fileName, RangeType rangeType, byte[] startRow,
          byte[] endRow) {
        fmb.add(new Destination(fileName, rangeType, copy(startRow), copy(endRow)));
        return this;
      }

      @Override
      public Builder loadFileTo(String fileName, RangeType rangeType, CharSequence startRow,
          CharSequence endRow) {
        fmb.add(new Destination(fileName, rangeType, copy(startRow), copy(endRow)));
        return this;
      }

      @Override
      public Builder addPlan(LoadPlan plan) {
        fmb.addAll(plan.getDestinations());
        return this;
      }

      @Override
      public LoadPlan build() {
        return new LoadPlan(fmb.build());
      }
    };
  }
}
