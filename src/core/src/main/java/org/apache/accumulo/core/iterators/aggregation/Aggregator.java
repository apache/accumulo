package org.apache.accumulo.core.iterators.aggregation;

import org.apache.accumulo.core.data.Value;

public interface  Aggregator {
    void reset();
    void collect(Value value);
    Value aggregate();
}
