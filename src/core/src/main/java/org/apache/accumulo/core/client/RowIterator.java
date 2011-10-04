package org.apache.accumulo.core.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;


/**
 * Group Key/Value pairs into lists corresponding to rows.
 *
 * Obviously, do not use this if your row will not fit into memory.
 */
public class RowIterator implements Iterator<List<Entry<Key, Value>>> {
    
    private final Iterator<Entry<Key, Value>> impl;
    private Entry<Key, Value> nextRow = null;
    
    /**
     * Create an iterator from an (ordered) sequence of KeyValue pairs. 
     * @param iterator
     */
    public RowIterator(Iterator<Entry<Key, Value>> iterator) {
        this.impl = iterator;
        if (iterator.hasNext())
            nextRow = iterator.next();
    }

    /**
     * Create an iterator from a Scanner.
     * @param scanner
     */
    public RowIterator(Scanner scanner) {
        this(scanner.iterator());
    }

    /**
     * Returns true if there is at least one more row to get.
     */
    @Override
    public boolean hasNext() {
        return nextRow != null;
    }

    /**
     * Fetch the next row.
     */
    @Override
    public List<Entry<Key, Value>> next() {
        final ArrayList<Entry<Key, Value>> result = new ArrayList<Entry<Key, Value>>();
        if (nextRow == null)
            throw new NoSuchElementException();
        
        final Text row = nextRow.getKey().getRow();
        result.add(nextRow);
        nextRow = null;
        while (impl.hasNext()) {
            nextRow = impl.next();
            if (nextRow.getKey().getRow().compareTo(row) == 0)
                result.add(nextRow);
            else
                break;
            nextRow = null;
        }
        return result;
    }

    /**
     * Unsupported.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
