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
package org.apache.accumulo.test.stress.random;

import org.apache.accumulo.core.cli.ClientOnDefaultTable;

import com.beust.jcommander.Parameter;

class WriteOptions extends ClientOnDefaultTable {
  static final String DEFAULT_TABLE = "stress_test";
  static final int DEFAULT_MIN = 1, DEFAULT_MAX = 128, DEFAULT_SPREAD = DEFAULT_MAX - DEFAULT_MIN;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--min-row-size", description = "minimum row size")
  Integer row_min;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--max-row-size", description = "maximum row size")
  Integer row_max;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--min-cf-size", description = "minimum column family size")
  Integer cf_min;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--max-cf-size", description = "maximum column family size")
  Integer cf_max;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--min-cq-size", description = "minimum column qualifier size")
  Integer cq_min;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--max-cq-size", description = "maximum column qualifier size")
  Integer cq_max;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--min-value-size", description = "minimum value size")
  Integer value_min;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--max-value-size", description = "maximum value size")
  Integer value_max;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--min-row-width", description = "minimum row width")
  Integer row_width_min;

  @Parameter(validateValueWith = IntArgValidator.class, names = "--max-row-width", description = "maximum row width")
  Integer row_width_max;

  @Parameter(names = "--clear-table", description = "clears the table before ingesting")
  boolean clear_table;

  @Parameter(names = "--row-seed", description = "seed for generating rows")
  int row_seed = 87;

  @Parameter(names = "--cf-seed", description = "seed for generating column families")
  int cf_seed = 7;

  @Parameter(names = "--cq-seed", description = "seed for generating column qualifiers")
  int cq_seed = 43;

  @Parameter(names = "--value-seed", description = "seed for generating values")
  int value_seed = 99;

  @Parameter(names = "--row-width-seed", description = "seed for generating the number of cells within a row (a row's \"width\")")
  int row_width_seed = 444;

  @Parameter(names = "--max-cells-per-mutation", description = "maximum number of cells per mutation; non-positive value implies no limit")
  int max_cells_per_mutation = -1;

  @Parameter(names = "--write-delay", description = "milliseconds to wait between writes")
  long write_delay = 0L;

  public WriteOptions(String table) {
    super(table);
  }

  public WriteOptions() {
    this(DEFAULT_TABLE);
  }

  private static int minOrDefault(Integer ref) {
    return ref == null ? DEFAULT_MIN : ref;
  }

  private static int calculateMax(Integer min_ref, Integer max_ref) {
    if (max_ref == null) {
      if (min_ref == null) {
        return DEFAULT_MAX;
      } else {
        return min_ref + DEFAULT_SPREAD;
      }
    } else {
      return max_ref;
    }
  }

  public void check() {
    checkPair("ROW", row_min, row_max);
    checkPair("COLUMN FAMILY", cf_min, cf_max);
    checkPair("COLUMN QUALIFIER", cq_min, cq_max);
    checkPair("VALUE", value_min, value_max);
  }

  public void checkPair(String label, Integer min_ref, Integer max_ref) {
    // we've already asserted that the numbers will either be
    // 1) null
    // 2) positive
    // need to verify that they're coherent here

    if (min_ref == null && max_ref != null) {
      // we don't support just specifying a max yet
      throw new IllegalArgumentException(String.format("[%s] Maximum value supplied, but no minimum. Must supply a minimum with a maximum value.", label));
    } else if (min_ref != null && max_ref != null) {
      // if a user supplied lower and upper bounds, we need to verify
      // that min <= max
      if (min_ref.compareTo(max_ref) > 0) {
        throw new IllegalArgumentException(String.format("[%s] Min value (%d) is greater than max value (%d)", label, min_ref, max_ref));
      }
    }
  }

  public int rowMin() {
    return minOrDefault(row_min);
  }

  public int rowMax() {
    return calculateMax(row_min, row_max);
  }

  public int cfMin() {
    return minOrDefault(cf_min);
  }

  public int cfMax() {
    return calculateMax(cf_min, cf_max);
  }

  public int cqMin() {
    return minOrDefault(cq_min);
  }

  public int cqMax() {
    return calculateMax(cq_min, cq_max);
  }

  public int valueMin() {
    return minOrDefault(value_min);
  }

  public int valueMax() {
    return calculateMax(value_min, value_max);
  }

  public int rowWidthMin() {
    return minOrDefault(row_width_min);
  }

  public int rowWidthMax() {
    return calculateMax(row_width_min, row_width_max);
  }
}
