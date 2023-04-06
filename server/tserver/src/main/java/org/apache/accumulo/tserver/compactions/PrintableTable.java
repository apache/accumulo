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
package org.apache.accumulo.tserver.compactions;

import java.util.stream.Stream;

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
    int widestRow = Stream.of(rows).mapToInt(String::length).max().getAsInt();

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < widestRow; i++) {
      sb.append(" ");
    }

    for (int i = 0; i < columns.length; i++) {
      sb.append("  C");
      sb.append(i + 1);
      sb.append("  ");
    }

    sb.append("\n");

    for (int i = 0; i < widestRow; i++) {
      sb.append("-");
    }

    for (int i = 0; i < columns.length; i++) {
      sb.append(" ---- ");
    }

    sb.append("\n");

    for (int r = 0; r < rows.length; r++) {
      sb.append(String.format("%" + widestRow + "s", rows[r]));

      int[] row = data[r];

      for (int c = 0; c < row.length; c++) {
        if (row[c] == 0) {
          sb.append("      ");
        } else {
          sb.append(String.format(" %4d ", row[c]));
        }
      }
      sb.append("\n");
    }

    sb.append("\n");

    for (int i = 0; i < columns.length; i++) {
      sb.append(" C");
      sb.append(i + 1);
      sb.append("='");
      sb.append(columns[i]);
      sb.append("'");
    }

    sb.append("\n");

    return sb.toString();
  }

  public static void main(String[] args) {
    String[] columns = {"small running", "small queued", "medium running", "medium queued",
        "large running", "large queued"};
    String[] rows = {"tablet 1", "tablet 2", "tablet 3"};

    int[][] data = {{0, 3, 1, 0, 0, 0}, {2, 0, 0, 0, 0, 0}, {2, 0, 4, 0, 0, 0}};

    System.out.println(new PrintableTable(columns, rows, data));
  }
}
