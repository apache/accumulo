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

import org.apache.accumulo.core.data.Mutation;

public class RandomMutations extends Stream<Mutation> {
  private final RandomByteArrays rows, column_families, column_qualifiers, values;
  private final RandomWithinRange row_widths;
  
  public RandomMutations(RandomByteArrays rows, RandomByteArrays column_families,
      RandomByteArrays column_qualifiers, RandomByteArrays values, RandomWithinRange row_widths) {
    this.rows = rows;
    this.column_families = column_families;
    this.column_qualifiers = column_qualifiers;
    this.values = values;
    this.row_widths = row_widths;
  }

  // TODO should we care about timestamps?
  @Override
  public Mutation next() {
    Mutation m = new Mutation(rows.next());
    final int cells = row_widths.next();
    for(int i = 0; i < cells; ++i) {
      m.put(column_families.next(), column_qualifiers.next(), values.next());
    }
    return m;
  }
}
