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
package org.apache.accumulo.monitor.util;

import org.apache.accumulo.monitor.util.celltypes.CellType;
import org.apache.accumulo.monitor.util.celltypes.StringType;

public class TableColumn<T> {
  private String title;
  private CellType<T> type;
  private String legend;

  public TableColumn(String title, CellType<T> type, String legend) {
    this.title = title;
    this.type = type != null ? type : new StringType<T>();
    this.legend = legend;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getTitle() {
    return title;
  }

  public String getLegend() {
    return legend;
  }

  public CellType<T> getCellType() {
    return type;
  }
}
