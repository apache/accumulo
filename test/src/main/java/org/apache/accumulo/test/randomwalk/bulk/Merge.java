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
package org.apache.accumulo.test.randomwalk.bulk;

import java.util.Arrays;
import java.util.Random;

import org.apache.accumulo.test.randomwalk.State;
import org.apache.hadoop.io.Text;

public class Merge extends SelectiveBulkTest {

  @Override
  protected void runLater(State state) throws Exception {
    Text[] points = getRandomTabletRange(state);
    log.info("merging " + rangeToString(points));
    state.getConnector().tableOperations().merge(Setup.getTableName(), points[0], points[1]);
    log.info("merging " + rangeToString(points) + " complete");
  }

  public static String rangeToString(Text[] points) {
    return "(" + (points[0] == null ? "-inf" : points[0]) + " -> " + (points[1] == null ? "+inf" : points[1]) + "]";
  }

  public static Text getRandomRow(Random rand) {
    return new Text(String.format(BulkPlusOne.FMT, (rand.nextLong() & 0x7fffffffffffffffl) % BulkPlusOne.LOTS));
  }

  public static Text[] getRandomTabletRange(State state) {
    Random rand = (Random) state.get("rand");
    Text points[] = {getRandomRow(rand), getRandomRow(rand),};
    Arrays.sort(points);
    if (rand.nextInt(10) == 0) {
      points[0] = null;
    }
    if (rand.nextInt(10) == 0) {
      points[1] = null;
    }
    if (rand.nextInt(20) == 0) {
      points[0] = null;
      points[1] = null;
    }
    return points;
  }

}
