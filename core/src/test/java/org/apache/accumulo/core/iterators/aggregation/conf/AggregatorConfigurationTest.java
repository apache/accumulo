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
package org.apache.accumulo.core.iterators.aggregation.conf;

import junit.framework.TestCase;

import org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.4
 */
@Deprecated
public class AggregatorConfigurationTest extends TestCase {
  public void testBinary() {
    Text colf = new Text();
    Text colq = new Text();

    for (int i = 0; i < 256; i++) {
      colf.append(new byte[] {(byte) i}, 0, 1);
      colq.append(new byte[] {(byte) (255 - i)}, 0, 1);
    }

    runTest(colf, colq);
    runTest(colf);
  }

  public void testBasic() {
    runTest(new Text("colf1"), new Text("cq2"));
    runTest(new Text("colf1"));
  }

  private void runTest(Text colf) {
    String encodedCols;
    PerColumnIteratorConfig ac3 = new PerColumnIteratorConfig(colf, "com.foo.SuperAgg");
    encodedCols = ac3.encodeColumns();
    PerColumnIteratorConfig ac4 = PerColumnIteratorConfig.decodeColumns(encodedCols, "com.foo.SuperAgg");

    assertEquals(colf, ac4.getColumnFamily());
    assertNull(ac4.getColumnQualifier());
  }

  private void runTest(Text colf, Text colq) {
    PerColumnIteratorConfig ac = new PerColumnIteratorConfig(colf, colq, "com.foo.SuperAgg");
    String encodedCols = ac.encodeColumns();
    PerColumnIteratorConfig ac2 = PerColumnIteratorConfig.decodeColumns(encodedCols, "com.foo.SuperAgg");

    assertEquals(colf, ac2.getColumnFamily());
    assertEquals(colq, ac2.getColumnQualifier());
  }

}
