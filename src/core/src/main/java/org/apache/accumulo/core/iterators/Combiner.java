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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@SuppressWarnings("deprecation")
public abstract class Combiner extends WrappingIterator implements OptionDescriber {
    static final Logger log = Logger.getLogger(Combiner.class);
    public static final String COLUMN_PREFIX = "column:";
    
    public static class ValueIterator implements Iterator<Value> {
        Key topKey;
        SortedKeyValueIterator<Key,Value> source;
        boolean hasNext;
        
        ValueIterator(SortedKeyValueIterator<Key,Value> source) {
            this.source = source;
            topKey = source.getTopKey();
            hasNext = _hasNext();
        }
        
        private boolean _hasNext() {
            return source.hasTop() && !source.getTopKey().isDeleted() && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
        }
        
        @Override
        public boolean hasNext() {
            return hasNext;
        }
        
        @Override
        public Value next() {
            if (!hasNext) throw new NoSuchElementException();
            Value topValue = source.getTopValue();
            try {
                source.next();
                hasNext = _hasNext();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return topValue;
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    Key topKey;
    Value topValue;
    
    public Combiner() {}
    
    @Override
    public Key getTopKey() {
        if (topKey == null) return super.getTopKey();
        return topKey;
    }
    
    @Override
    public Value getTopValue() {
        if (topKey == null) return super.getTopValue();
        return topValue;
    }
    
    @Override
    public boolean hasTop() {
        return topKey != null || super.hasTop();
    }
    
    @Override
    public void next() throws IOException {
        if (topKey != null) {
            topKey = null;
            topValue = null;
        } else {
            super.next();
        }
        
        findTop();
    }
    
    private Key workKey = new Key();
    
    private void findTop() throws IOException {
        // check if aggregation is needed
        if (super.hasTop()) {
            workKey.set(super.getTopKey());
            if (combiners.isEmpty() || combiners.contains(workKey)) {
                if (workKey.isDeleted()) return;
                topKey = workKey;
                Iterator<Value> viter = new ValueIterator(getSource());
                topValue = reduce(topKey, viter);
                while (viter.hasNext())
                    viter.next();
            }
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // do not want to seek to the middle of a value that should be
        // combined...
        
        Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
        
        super.seek(seekRange, columnFamilies, inclusive);
        findTop();
        
        if (range.getStartKey() != null) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
                    && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
                // the value has a more recent time stamp, so
                // pass it up
                // log.debug("skipping "+getTopKey());
                next();
            }
            
            while (hasTop() && range.beforeStartKey(getTopKey())) {
                next();
            }
        }
    }
    
    public abstract Value reduce(Key key, Iterator<Value> iter);
    
    private ColumnSet combiners;
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        List<String> colOpts = new ArrayList<String>();
        for (Entry<String,String> opt : options.entrySet()) {
            if ((opt.getKey().startsWith(COLUMN_PREFIX))) {
                colOpts.add(opt.getKey().substring(COLUMN_PREFIX.length()));
            }
        }
        combiners = new ColumnSet(colOpts);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("comb", "Combiners apply reduce functions to values with identical keys", null,
                Collections.singletonList("<columnName> null"));
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }
    
    public static void addColumn(Text colf, Text colq, IteratorSetting is) {
        String column = PerColumnIteratorConfig.encodeColumns(colf, colq);
        is.addOption(COLUMN_PREFIX + column, "");
    }
}
