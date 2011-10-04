package org.apache.accumulo.core.client.mock;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


class MockEntry implements Entry<Key, Value> {
    Key key;
    Value value;
    
    public MockEntry(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public Key getKey() {
        return key;
    }

    @Override
    public Value getValue() {
        return value;
    }

    @Override
    public Value setValue(Value value) {
        Value result = this.value;
        this.value = value;
        return result;
    }

    @Override
    public String toString() {
        return key.toString() + " -> " + value.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MockEntry)) {
            return false;
        }
        MockEntry entry = (MockEntry) obj;
        return key.equals(entry.key) && value.equals(entry.value);
    }
    
    
}