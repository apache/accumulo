package org.apache.accumulo.core.util;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 * A utility that mimics String.intern() for any immutable object type (including String).
 */
public class Interner<T> {
  private final WeakHashMap<T,WeakReference<T>> internTable = new WeakHashMap<>();

  public synchronized T intern(T item) {
    WeakReference<T> ref = internTable.get(item);
    if (ref != null) {
      T oldItem = ref.get();
      if (oldItem != null) {
        return oldItem;
      }
    }
    internTable.put(item, new WeakReference<>(item));
    return item;
  }

  // for testing
  synchronized int size() {
    return internTable.size();
  }
}
