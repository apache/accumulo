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
package org.apache.accumulo.examples.dirlist;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.iterators.aggregation.NumSummation;

public class StringArraySummation implements Aggregator {
    List<Long> sums = new ArrayList<Long>();
    
    @Override
    public void reset() {
        sums.clear();
    }
    
    @Override
    public void collect(Value value) {
        String[] longs = value.toString().split(",");
        int i;
        for (i = 0; i < sums.size() && i < longs.length; i++) {
            sums.set(i, NumSummation.safeAdd(sums.get(i).longValue(), Long.parseLong(longs[i])));
        }
        for (; i < longs.length; i++) {
            sums.add(Long.parseLong(longs[i]));
        }
    }
    
    @Override
    public Value aggregate() {
        return new Value(longArrayToStringBytes(sums.toArray(new Long[sums.size()])));
    }
    
    public static byte[] longArrayToStringBytes(Long[] l) {
        if (l.length == 0) return new byte[] {};
        StringBuilder sb = new StringBuilder(Long.toString(l[0]));
        for (int i = 1; i < l.length; i++) {
            sb.append(",");
            sb.append(Long.toString(l[i]));
        }
        return sb.toString().getBytes();
    }
}
