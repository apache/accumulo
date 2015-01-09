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
package org.apache.accumulo.monitor.util.celltypes;

public class PercentageType extends CellType<Double> {

  @Override
  public int compare(Double o1, Double o2) {
    return o1.compareTo(o2);
  }

  @Override
  public String alignment() {
    return "right";
  }

  @Override
  public String format(Object obj) {
    if (obj == null)
      return "-";

    return String.format("%.0f%s", 100 * (Double) obj, "%");

  }

}
