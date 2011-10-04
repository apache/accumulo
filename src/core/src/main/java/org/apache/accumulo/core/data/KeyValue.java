package org.apache.accumulo.core.data;

import java.util.Map;

public class KeyValue implements Map.Entry<Key, Value> {
    
    public Key key;
    public byte[] value;

    public KeyValue(
      Key key,
      byte[] value)
    {
      this.key = key;
      this.value = value;
    }

    @Override
    public Key getKey() {
        return key;
    }

    @Override
    public Value getValue() {
        return new Value(value);
    }

    @Override
    public Value setValue(Value value) {
        throw new UnsupportedOperationException();
    }

    public String toString(){
    	return key+" "+new String(value);
    }
    
}

