/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.compactions;

import java.util.Arrays;
import java.util.stream.IntStream;

public class PrintableTable {
  private String[] columns;
  private String[] rows;
  private int[][] data;

  PrintableTable(String[] columns, String[] rows, int[][] data) {
    this.columns = columns;
    this.rows = rows;
    this.data = data;
  }

  @Override
  public String toString() {
    int widestRow = Arrays.asList(rows).stream().mapToInt(String::length).max().getAsInt();

    StringBuilder sb = new StringBuilder();

    IntStream.range(0, widestRow).forEach(i -> sb.append(" "));
    IntStream.range(0, columns.length).forEach(i -> sb.append("  C").append(i + 1).append("  "));
    sb.append("\n");
    IntStream.range(0, widestRow).forEach(i -> sb.append("-"));
    IntStream.range(0, columns.length).forEach(i -> sb.append(" ---- "));
    sb.append("\n");

    for (int r = 0; r < rows.length; r++) {
      sb.append(String.format("%" + widestRow + "s", rows[r]));

      int[] row = data[r];

      for (int element : row) {
        if (element == 0)
          sb.append("      ");
        else
          sb.append(String.format(" %4d ", element));
      }
      sb.append("\n");
    }

    sb.append("\n");

    IntStream.range(0, columns.length).forEach(i -> {
      sb.append(" C").append(i + 1).append("='").append(columns[i]).append("'");
    });
    sb.append("\n");

    return sb.toString();
  }

  public static void main(String[] args) {
    String[] columns = {"small running", "small queued", "medium running", "medium queued",
        "large running", "large queued"};
    String[] rows = {"tablet 1", "tablet 2", "tablet 3"};

    int[][] data = {{0, 3, 1, 0, 0, 0}, {2, 0, 0, 0, 0, 0}, {2, 0, 4, 0, 0, 0}};

    System.out.println(new PrintableTable(columns, rows, data).toString());
  }
}
