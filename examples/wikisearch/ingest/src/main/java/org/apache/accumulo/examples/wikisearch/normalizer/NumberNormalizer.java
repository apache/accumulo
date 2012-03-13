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
package org.apache.accumulo.examples.wikisearch.normalizer;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.lucene.util.NumericUtils;

public class NumberNormalizer implements Normalizer {
  
  public String normalizeFieldValue(String field, Object value) {
    if (NumberUtils.isNumber(value.toString())) {
      Number n = NumberUtils.createNumber(value.toString());
      if (n instanceof Integer)
        return NumericUtils.intToPrefixCoded((Integer) n);
      else if (n instanceof Long)
        return NumericUtils.longToPrefixCoded((Long) n);
      else if (n instanceof Float)
        return NumericUtils.floatToPrefixCoded((Float) n);
      else if (n instanceof Double)
        return NumericUtils.doubleToPrefixCoded((Double) n);
      else
        throw new IllegalArgumentException("Unhandled numeric type: " + n.getClass());
    } else {
      throw new IllegalArgumentException("Value is not a number: " + value);
    }
  }
  
}
