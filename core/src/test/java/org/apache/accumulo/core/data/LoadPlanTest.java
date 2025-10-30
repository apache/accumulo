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
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.LoadPlan.Destination;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LoadPlanTest {
  @Test
  public void testBadRange1() {
    assertThrows(IllegalArgumentException.class,
        () -> LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, "a", "a").build());
  }

  @Test
  public void testBadRange2() {
    assertThrows(IllegalArgumentException.class,
        () -> LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, "b", "a").build());
  }

  @Test
  public void testBadRange3() {
    assertThrows(IllegalArgumentException.class,
        () -> LoadPlan.builder().loadFileTo("f1.rf", RangeType.FILE, "b", "a").build());
  }

  @Test
  public void testBadRange4() {
    assertThrows(IllegalArgumentException.class,
        () -> LoadPlan.builder().loadFileTo("f1.rf", RangeType.FILE, null, "a").build());
  }

  @Test
  public void testBadRange5() {
    assertThrows(IllegalArgumentException.class,
        () -> LoadPlan.builder().loadFileTo("f1.rf", RangeType.FILE, "a", null).build());
  }

  @Test
  public void testTypes() {
    LoadPlan loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.FILE, "1112", "1145")
        .loadFileTo("f2.rf", RangeType.FILE, "abc".getBytes(UTF_8), "def".getBytes(UTF_8))
        .loadFileTo("f3.rf", RangeType.FILE, new Text("368"), new Text("479"))
        .loadFileTo("f4.rf", RangeType.TABLE, null, "aaa")
        .loadFileTo("f5.rf", RangeType.TABLE, "yyy", null)
        .loadFileTo("f6.rf", RangeType.TABLE, null, "bbb".getBytes(UTF_8))
        .loadFileTo("f7.rf", RangeType.TABLE, "www".getBytes(UTF_8), null)
        .loadFileTo("f8.rf", RangeType.TABLE, null, new Text("ccc"))
        .loadFileTo("f9.rf", RangeType.TABLE, new Text("xxx"), null)
        .loadFileTo("fa.rf", RangeType.TABLE, "1138", "1147")
        .loadFileTo("fb.rf", RangeType.TABLE, "heg".getBytes(UTF_8), "klt".getBytes(UTF_8))
        .loadFileTo("fc.rf", RangeType.TABLE, new Text("agt"), new Text("ctt"))
        .addPlan(
            LoadPlan.builder().loadFileTo("fd.rf", RangeType.TABLE, (String) null, null).build())
        .build();

    Set<String> expected = new HashSet<>();
    expected.add("f1.rf:FILE:1112:1145");
    expected.add("f2.rf:FILE:abc:def");
    expected.add("f3.rf:FILE:368:479");
    expected.add("f4.rf:TABLE:null:aaa");
    expected.add("f5.rf:TABLE:yyy:null");
    expected.add("f6.rf:TABLE:null:bbb");
    expected.add("f7.rf:TABLE:www:null");
    expected.add("f8.rf:TABLE:null:ccc");
    expected.add("f9.rf:TABLE:xxx:null");
    expected.add("fa.rf:TABLE:1138:1147");
    expected.add("fb.rf:TABLE:heg:klt");
    expected.add("fc.rf:TABLE:agt:ctt");
    expected.add("fd.rf:TABLE:null:null");

    Set<String> actual =
        loadPlan.getDestinations().stream().map(LoadPlanTest::toString).collect(toSet());

    assertEquals(expected, actual);

    var loadPlan2 = LoadPlan.fromJson(loadPlan.toJson());
    Set<String> actual2 =
        loadPlan2.getDestinations().stream().map(LoadPlanTest::toString).collect(toSet());
    assertEquals(expected, actual2);
  }

  @Test
  public void testJson() throws Exception {
    var loadPlan = LoadPlan.builder().build();
    assertEquals(0, loadPlan.getDestinations().size());
    assertEquals("{\"destinations\":[]}", loadPlan.toJson());

    var builder = LoadPlan.builder();
    builder.loadFileTo("f1.rf", RangeType.TABLE, null, "003");
    builder.loadFileTo("f2.rf", RangeType.FILE, "004", "007");
    builder.loadFileTo("f1.rf", RangeType.TABLE, "005", "006");
    builder.loadFileTo("f3.rf", RangeType.TABLE, new byte[] {0, 1, 2, 3, 4, 5, 6}, null);
    String json = builder.build().toJson();

    String b64003 = Base64.getUrlEncoder().encodeToString("003".getBytes(UTF_8));
    String b64004 = Base64.getUrlEncoder().encodeToString("004".getBytes(UTF_8));
    String b64005 = Base64.getUrlEncoder().encodeToString("005".getBytes(UTF_8));
    String b64006 = Base64.getUrlEncoder().encodeToString("006".getBytes(UTF_8));
    String b64007 = Base64.getUrlEncoder().encodeToString("007".getBytes(UTF_8));
    String b64binary = Base64.getUrlEncoder().encodeToString(new byte[] {0, 1, 2, 3, 4, 5, 6});

    String expected = "{'destinations':[{'fileName':'f1.rf','startRow':null,'endRow':'" + b64003
        + "','rangeType':'TABLE'},{'fileName':'f2.rf','startRow':'" + b64004 + "','endRow':'"
        + b64007 + "','rangeType':'FILE'},{'fileName':'f1.rf','startRow':'" + b64005
        + "','endRow':'" + b64006 + "','rangeType':'TABLE'},{'fileName':'f3.rf','startRow':'"
        + b64binary + "','endRow':null,'rangeType':'TABLE'}]}";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node1 = mapper.readTree(expected.replace("'", "\""));
    JsonNode node2 = mapper.readTree(json);

    assertEquals(node1, node2);
  }

  @Test
  public void testIllegalJson() {
    assertThrows(NullPointerException.class, () -> LoadPlan.fromJson(null));

    List<String> illegalJson = new ArrayList<>();
    // Test json with extraneous stuff in it
    illegalJson.add("{'dest':[],'destinations':[]}");
    // lets try XML
    illegalJson.add("<destinations></destinations>");
    // try an empty string
    illegalJson.add("");
    illegalJson.add(" ");
    // try incomplete json
    illegalJson.add("{'destinations':[{'fileName':'f1.rf'");
    // try extra field in the destination
    illegalJson.add(
        "{'destinations':[{'host':'localhost',fileName':'f1.rf','startRow':null,'endRow':'g','rangeType':'TABLE'}]}");
    // try an illegal range type
    illegalJson.add(
        "{'destinations':[{'fileName':'f1.rf','startRow':null,'endRow':null,'rangeType':'LARGE'}]}");
    // try object value instead of array for destinations field
    illegalJson.add(
        "{'destinations':{'fileName':'f1.rf','startRow':null,'endRow': null,'rangeType':'TABLE'}}");
    // try array of array value instead of array for destinations field
    illegalJson.add(
        "{'destinations':[[{'fileName':'f1.rf','startRow':null,'endRow': null,'rangeType':'TABLE'}]]}");
    // try a row value that is not valid base 64
    illegalJson.add(
        "{'destinations':[{'fileName':'f1.rf','startRow':null,'endRow': '~!@#$%^&*()_+','rangeType':'TABLE'}]}");

    illegalJson.forEach(json -> assertThrows(IllegalArgumentException.class,
        () -> LoadPlan.fromJson(json.replace("'", "\""))));
  }

  @Test
  public void testTableSplits() {
    assertThrows(IllegalArgumentException.class,
        () -> new LoadPlan.TableSplits(new Text("004"), new Text("004")));
    assertThrows(IllegalArgumentException.class,
        () -> new LoadPlan.TableSplits(new Text("004"), new Text("003")));
  }

  private static String toString(Destination d) {
    return d.getFileName() + ":" + d.getRangeType() + ":" + toString(d.getStartRow()) + ":"
        + toString(d.getEndRow());
  }

  private static String toString(byte[] r) {
    return r == null ? null : new String(r, UTF_8);
  }

  public static Set<String> toString(Collection<Destination> destinations) {
    return destinations.stream().map(d -> toString(d)).collect(Collectors.toSet());
  }

  @Test
  public void testHashCode() {
    // Testing consistency
    LoadPlan.TableSplits tableSplits =
        new LoadPlan.TableSplits(new Text("text1"), new Text("text2"));
    int hashCode1 = tableSplits.hashCode();
    int hashCode2 = tableSplits.hashCode();
    assertEquals(hashCode1, hashCode2);

    // Testing equality
    LoadPlan.TableSplits tableOne = new LoadPlan.TableSplits(new Text("text1"), new Text("text2"));
    LoadPlan.TableSplits tableTwo = new LoadPlan.TableSplits(new Text("text1"), new Text("text2"));
    assertEquals(tableOne.hashCode(), tableTwo.hashCode());

    // Testing even distribution
    List<LoadPlan.TableSplits> tables = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      tables.add(new LoadPlan.TableSplits(new Text("text1" + i), new Text("text2" + i)));
    }
    Set<Integer> hashCodes = new HashSet<>();
    for (LoadPlan.TableSplits tabs : tables) {
      hashCodes.add(tabs.hashCode());
    }
    assertEquals(tables.size(), hashCodes.size(), 10);
  }
}
