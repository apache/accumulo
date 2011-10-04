package org.apache.accumulo.core.client.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;


public class MockBatchScanner extends MockScannerBase implements BatchScanner {
    
    final List<Range> ranges = new ArrayList<Range>();

    public MockBatchScanner(MockTable mockTable, Authorizations authorizations) {
        super(mockTable, authorizations);
    }

    @Override
    public void setRanges(Collection<Range> ranges) {
        this.ranges.clear();
        this.ranges.addAll(ranges);
    }
    
    static class RangesFilter extends Filter {
        List<Range> ranges;
        
        public RangesFilter(SortedKeyValueIterator<Key,Value> iterator, List<Range> ranges) {
        	super(iterator);
            this.ranges = ranges;
        }

        @Override
        public boolean accept(Key k, Value v) {
            for (Range r : ranges) {
                if (r.contains(k))
                    return true;
            }
            return false;
        }
    }
    
    @Override
    public Iterator<Entry<Key, Value>> iterator() {
        SortedKeyValueIterator<Key, Value> i = new SortedMapIterator(table.table);
        try {
            i = createFilter(i);
            i.seek(new Range(), createColumnBSS(fetchedColumns), !fetchedColumns.isEmpty());
            return new IteratorAdapter(new RangesFilter(i, ranges)); 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

}
