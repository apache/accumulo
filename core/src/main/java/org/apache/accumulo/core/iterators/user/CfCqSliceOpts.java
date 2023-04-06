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
package org.apache.accumulo.core.iterators.user;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.hadoop.io.Text;

public class CfCqSliceOpts {
  public static final String OPT_MIN_CF = "minCf";
  public static final String OPT_MIN_CF_DESC = "UTF-8 encoded string"
      + " representing minimum column family. Optional parameter. If minCf and minCq"
      + " are undefined, the column slice will start at the first column of each row."
      + " If you want to do an exact match on column families, it's more efficient to"
      + " leave minCf and maxCf undefined and use the scanner's fetchColumnFamily method.";

  public static final String OPT_MIN_CQ = "minCq";
  public static final String OPT_MIN_CQ_DESC = "UTF-8 encoded string"
      + " representing minimum column qualifier. Optional parameter. If minCf and"
      + " minCq are undefined, the column slice will start at the first column of each row.";

  public static final String OPT_MAX_CF = "maxCf";
  public static final String OPT_MAX_CF_DESC = "UTF-8 encoded string"
      + " representing maximum column family. Optional parameter. If minCf and minCq"
      + " are undefined, the column slice will start at the first column of each row."
      + " If you want to do an exact match on column families, it's more efficient to"
      + " leave minCf and maxCf undefined and use the scanner's fetchColumnFamily method.";

  public static final String OPT_MAX_CQ = "maxCq";
  public static final String OPT_MAX_CQ_DESC = "UTF-8 encoded string"
      + " representing maximum column qualifier. Optional parameter. If maxCf and"
      + " MaxCq are undefined, the column slice will end at the last column of each row.";

  public static final String OPT_MIN_INCLUSIVE = "minInclusive";
  public static final String OPT_MIN_INCLUSIVE_DESC = "UTF-8 encoded string"
      + " indicating whether to include the minimum column in the slice range."
      + " Optional parameter, default is true.";

  public static final String OPT_MAX_INCLUSIVE = "maxInclusive";
  public static final String OPT_MAX_INCLUSIVE_DESC = "UTF-8 encoded string"
      + " indicating whether to include the maximum column in the slice range."
      + " Optional parameter, default is true.";

  Text minCf;
  Text minCq;

  Text maxCf;
  Text maxCq;

  boolean minInclusive;
  boolean maxInclusive;

  public CfCqSliceOpts(CfCqSliceOpts o) {
    minCf = new Text(o.minCf);
    minCq = new Text(o.minCq);
    maxCf = new Text(o.maxCf);
    maxCq = new Text(o.maxCq);
    minInclusive = o.minInclusive;
    maxInclusive = o.maxInclusive;
  }

  public CfCqSliceOpts(Map<String,String> options) {
    String optStr = options.get(OPT_MIN_CF);
    minCf = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

    optStr = options.get(OPT_MIN_CQ);
    minCq = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

    optStr = options.get(OPT_MAX_CF);
    maxCf = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

    optStr = options.get(OPT_MAX_CQ);
    maxCq = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

    optStr = options.get(OPT_MIN_INCLUSIVE);
    minInclusive =
        optStr == null || optStr.isEmpty() ? true : Boolean.valueOf(options.get(OPT_MIN_INCLUSIVE));

    optStr = options.get(OPT_MAX_INCLUSIVE);
    maxInclusive =
        optStr == null || optStr.isEmpty() ? true : Boolean.valueOf(options.get(OPT_MAX_INCLUSIVE));
  }

  static class Describer implements OptionDescriber {
    @Override
    public OptionDescriber.IteratorOptions describeOptions() {
      Map<String,String> options = new HashMap<>();
      options.put(OPT_MIN_CF, OPT_MIN_CF_DESC);
      options.put(OPT_MIN_CQ, OPT_MIN_CQ_DESC);
      options.put(OPT_MAX_CF, OPT_MAX_CF_DESC);
      options.put(OPT_MAX_CQ, OPT_MAX_CQ_DESC);
      options.put(OPT_MIN_INCLUSIVE, OPT_MIN_INCLUSIVE_DESC);
      options.put(OPT_MAX_INCLUSIVE, OPT_MAX_INCLUSIVE_DESC);
      return new OptionDescriber.IteratorOptions("ColumnSliceFilter",
          "Returns all key/value pairs where the column is between the specified values", options,
          Collections.emptyList());
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
      // if you don't specify a max CF and a max CQ, that means there's no upper bounds to the
      // slice. In that case
      // you must not set max inclusive to false.
      CfCqSliceOpts o = new CfCqSliceOpts(options);
      boolean boundsOk = true;
      boolean upperBoundsExist = o.maxCf.getLength() > 0 && o.maxCq.getLength() > 0;
      if (upperBoundsExist) {
        boundsOk = o.maxInclusive;
      }
      boolean cqRangeOk = o.maxCq.getLength() == 0 || (o.minCq.compareTo(o.maxCq) < 1);
      boolean cfRangeOk = o.maxCf.getLength() == 0 || (o.minCf.compareTo(o.maxCf) < 1);
      return boundsOk && cqRangeOk && cfRangeOk;
    }
  }

}
