package org.apache.accumulo.server.util;

import java.util.HashMap;

/**
 * A HashMap that returns a default value if the key is not stored in the map.
 * 
 * A zero-argument constructor of the default object's class is used, otherwise the default object is used.
 */
public class DefaultMap<K, V> extends HashMap<K, V> {
    private static final long serialVersionUID = 1L;
    V dfault;
    
    public DefaultMap(V dfault) { this.dfault = dfault; }

    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        V result = super.get(key);
        if (result == null) {
            try {
                super.put((K)key, result = construct());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private V construct() {
        try {
            return (V)dfault.getClass().newInstance();
        } catch (Exception ex) {
            return dfault;
        }
    }
}
