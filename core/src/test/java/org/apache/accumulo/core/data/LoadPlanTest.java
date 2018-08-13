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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.LoadPlan.Destination;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class LoadPlanTest {
  @Test(expected = IllegalArgumentException.class)
  public void testBadRange1() {
    LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLET, "a", "a").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadRange2() {
    LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLET, "b", "a").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadRange3() {
    LoadPlan.builder().loadFileTo("f1.rf", RangeType.DATA, "b", "a").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadRange4() {
    LoadPlan.builder().loadFileTo("f1.rf", RangeType.DATA, null, "a").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadRange5() {
    LoadPlan.builder().loadFileTo("f1.rf", RangeType.DATA, "a", null).build();
  }

  @Test
  public void testTypes() {
    LoadPlan loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.DATA, "1112", "1145")
        .loadFileTo("f2.rf", RangeType.DATA, "abc".getBytes(UTF_8), "def".getBytes(UTF_8))
        .loadFileTo("f3.rf", RangeType.DATA, new Text("368"), new Text("479"))
        .loadFileTo("f4.rf", RangeType.TABLET, null, "aaa")
        .loadFileTo("f5.rf", RangeType.TABLET, "yyy", null)
        .loadFileTo("f6.rf", RangeType.TABLET, null, "bbb".getBytes(UTF_8))
        .loadFileTo("f7.rf", RangeType.TABLET, "www".getBytes(UTF_8), null)
        .loadFileTo("f8.rf", RangeType.TABLET, null, new Text("ccc"))
        .loadFileTo("f9.rf", RangeType.TABLET, new Text("xxx"), null)
        .loadFileTo("fa.rf", RangeType.TABLET, "1138", "1147")
        .loadFileTo("fb.rf", RangeType.TABLET, "heg".getBytes(UTF_8), "klt".getBytes(UTF_8))
        .loadFileTo("fc.rf", RangeType.TABLET, new Text("agt"), new Text("ctt"))
        .addPlan(
            LoadPlan.builder().loadFileTo("fd.rf", RangeType.TABLET, (String) null, null).build())
        .build();

    Set<String> expected = new HashSet<>();
    expected.add("f1.rf:DATA:1112:1145");
    expected.add("f2.rf:DATA:abc:def");
    expected.add("f3.rf:DATA:368:479");
    expected.add("f4.rf:TABLET:null:aaa");
    expected.add("f5.rf:TABLET:yyy:null");
    expected.add("f6.rf:TABLET:null:bbb");
    expected.add("f7.rf:TABLET:www:null");
    expected.add("f8.rf:TABLET:null:ccc");
    expected.add("f9.rf:TABLET:xxx:null");
    expected.add("fa.rf:TABLET:1138:1147");
    expected.add("fb.rf:TABLET:heg:klt");
    expected.add("fc.rf:TABLET:agt:ctt");
    expected.add("fd.rf:TABLET:null:null");

    Set<String> actual = loadPlan.getDestinations().stream().map(LoadPlanTest::toString)
        .collect(toSet());

    Assert.assertEquals(expected, actual);

  }

  private static String toString(Destination d) {
    return d.getFileName() + ":" + d.getRangeType() + ":" + toString(d.getStartRow()) + ":"
        + toString(d.getEndRow());
  }

  private static String toString(byte[] r) {
    return r == null ? null : new String(r, UTF_8);
  }
}
