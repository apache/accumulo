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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class AgeOffFilter extends Filter implements OptionDescriber {
    private static final String TTL = "ttl";
    private long threshold;
    private long currentTime;
    
    public AgeOffFilter() {}
    
    public AgeOffFilter(SortedKeyValueIterator<Key,Value> iterator, long threshold, long currentTime) {
        super(iterator);
        this.threshold = threshold;
        this.currentTime = currentTime;
    }
    
    @Override
    public boolean accept(Key k, Value v) {
        if (currentTime - k.getTimestamp() > threshold) return false;
        return true;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        threshold = -1;
        if (options == null) throw new IllegalArgumentException(TTL + " must be set for AgeOffFilter");
        
        String ttl = options.get(TTL);
        if (ttl == null) throw new IllegalArgumentException(TTL + " must be set for AgeOffFilter");
        
        threshold = Long.parseLong(ttl);
        
        String time = options.get("currentTime");
        if (time != null) currentTime = Long.parseLong(time);
        else currentTime = System.currentTimeMillis();
        
        // add sanity checks for threshold and currentTime?
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new AgeOffFilter(getSource(), threshold, currentTime);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption(TTL, "time to live (milliseconds)");
        io.addNamedOption("currentTime", "if set, use the given value as the absolute time in milliseconds as the current time of day");
        io.setName("ageoff");
        io.setDescription("AgeOffFilter removes entries with timestamps more than <ttl> milliseconds old");
        return io;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        try {
            Long.parseLong(options.get(TTL));
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}
