package org.apache.accumulo.tserver;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.impl.TabletType;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.log4j.Logger;

/**
 * This little class keeps track of writes in progress and allows readers to wait for writes that started before the read. It assumes that the operation ids
 * are monotonically increasing.
 *
 */
class WriteTracker {
  private static Logger log = Logger.getLogger(WriteTracker.class);
  
  private static final AtomicLong operationCounter = new AtomicLong(1);
  private final Map<TabletType,TreeSet<Long>> inProgressWrites = new EnumMap<TabletType,TreeSet<Long>>(TabletType.class);

  WriteTracker() {
    for (TabletType ttype : TabletType.values()) {
      inProgressWrites.put(ttype, new TreeSet<Long>());
    }
  }

  synchronized long startWrite(TabletType ttype) {
    long operationId = operationCounter.getAndIncrement();
    inProgressWrites.get(ttype).add(operationId);
    return operationId;
  }

  synchronized void finishWrite(long operationId) {
    if (operationId == -1)
      return;

    boolean removed = false;

    for (TabletType ttype : TabletType.values()) {
      removed = inProgressWrites.get(ttype).remove(operationId);
      if (removed)
        break;
    }

    if (!removed) {
      throw new IllegalArgumentException("Attempted to finish write not in progress,  operationId " + operationId);
    }

    this.notifyAll();
  }

  synchronized void waitForWrites(TabletType ttype) {
    long operationId = operationCounter.getAndIncrement();
    while (inProgressWrites.get(ttype).floor(operationId) != null) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        log.error(e, e);
      }
    }
  }

  public long startWrite(Set<Tablet> keySet) {
    if (keySet.size() == 0)
      return -1;

    List<KeyExtent> extents = new ArrayList<KeyExtent>(keySet.size());

    for (Tablet tablet : keySet)
      extents.add(tablet.getExtent());

    return startWrite(TabletType.type(extents));
  }
}