package org.apache.accumulo.server.test.functional;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;

public class BadAggregator implements Aggregator {

    @Override
    public Value aggregate() {
        throw new IllegalStateException();
    }

    @Override
    public void collect(Value value) {
        throw new IllegalStateException();
    }

    @Override
    public void reset() {
    }

}
