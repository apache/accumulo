package org.apache.accumulo.core.client.mock;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;

public class TransformIterator extends WrappingIterator {

  @Override
  public Key getTopKey() {
    Key k = getSource().getTopKey();
    return new Key(new Text(k.getRow().toString().toLowerCase()), k.getColumnFamily(), k.getColumnQualifier(), k.getColumnVisibility(), k.getTimestamp());
  }
}


