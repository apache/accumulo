package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filterer;
import org.apache.accumulo.core.iterators.Predicate;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class GenericFilterer extends WrappingIterator implements Filterer<Key,Value> {
  
  private ArrayList<Predicate<Key,Value>> filters = new ArrayList<Predicate<Key,Value>>();
  
  private Key topKey;
  private Value topValue;
  
  public GenericFilterer(SortedKeyValueIterator<Key,Value> source) {
    setSource(source);
  }

  public GenericFilterer() {
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    topKey = null;
    topValue = null;
    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  public void next() throws IOException {
    topKey = null;
    topValue = null;
    super.next();
  }

  @Override
  public Key getTopKey() {
    if(topKey == null)
      hasTop();
    return topKey;
  }
  
  @Override
  public Value getTopValue() {
    if(topValue == null)
      hasTop();
    return topValue;
  }
  
  @Override
  public boolean hasTop() {
    if(topKey == null)
    {
      while(super.hasTop())
      {
        topKey = super.getTopKey();
        topValue = super.getTopValue();
        // check all the filters to see if we found a valid key/value pair
        boolean keep = true;
        for(Predicate<Key,Value> filter: filters)
        {
          if(!filter.evaluate(topKey, topValue))
          {
            keep = false;
            try {
              super.next();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            break;
          }
        }
        if(keep == true)
          return true;
      }
      // ran out of key/value pairs
      topKey = null;
      topValue = null;
      return false;
    }
    else
    {
      return true;
    }
  }
  
  @Override
  public void applyFilter(Predicate<Key,Value> filter, boolean required) {
    filters.add(filter);
    if(getSource() instanceof Filterer)
    {
      Filterer<Key,Value> source = (Filterer<Key,Value>)getSource();
      source.applyFilter(filter, false);
    }
  }
}
