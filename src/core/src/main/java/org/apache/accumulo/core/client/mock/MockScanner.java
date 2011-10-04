package org.apache.accumulo.core.client.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FilteringIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.filter.Filter;
import org.apache.accumulo.core.security.Authorizations;


public class MockScanner extends MockScannerBase implements Scanner {
    
    int timeOut = 0;
    int batchSize = 0;
    Range range = new Range();
    
    MockScanner(MockTable table, Authorizations auths) {
        super(table, auths);
    }

    @Override
    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    @Override
    public int getTimeOut() {
        return timeOut;
    }

    @Override
    public void setRange(Range range) {
        this.range = range;
    }

    @Override
    public Range getRange() {
        return this.range;
    }

    @Override
    public void setBatchSize(int size) {
        this.batchSize = size;
    }

    @Override
    public int getBatchSize() {
        return this.batchSize;
    }

    @Override
    public void enableIsolation() {
    }

    @Override
    public void disableIsolation() {
    }
    
    static class RangeFilter implements Filter {
        Range range;
        public RangeFilter(Range range) {
            this.range = range;
        }
        @Override
        public void init(Map<String, String> options) {
        }

        @Override
        public boolean accept(Key k, Value v) {
            return range.contains(k);
        }
    }
    
    @Override
    public Iterator<Entry<Key, Value>> iterator() {
        SortedKeyValueIterator<Key, Value> i = new SortedMapIterator(table.table);
        try {
            i = createFilter(i);
            i.seek(range, createColumnBSS(columns), !columns.isEmpty());
            return new IteratorAdapter(new FilteringIterator(i, Collections.singletonList(new RangeFilter(range))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }

}
