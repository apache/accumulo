/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.htrace.wrappers.TraceRunnable;

/**
 * {@link TraceRunnable} objects placed in this queue <b>must</b> wrap a {@link Runnable} which is
 * also {@link Comparable}
 */
class CompactionQueue extends AbstractQueue<TraceRunnable> implements BlockingQueue<TraceRunnable> {

  private List<TraceRunnable> task = new LinkedList<>();

  @SuppressWarnings("unchecked")
  private static final Comparator<TraceRunnable> ELEMENT_COMPARATOR =
      (o1, o2) -> ((Comparable<Runnable>) o1.getRunnable()).compareTo(o2.getRunnable());

  @Override
  public synchronized TraceRunnable poll() {
    if (task.isEmpty())
      return null;

    TraceRunnable min = Collections.min(task, ELEMENT_COMPARATOR);
    Iterator<TraceRunnable> iterator = task.iterator();
    while (iterator.hasNext()) {
      if (iterator.next() == min) {
        iterator.remove();
        return min;
      }
    }
    throw new IllegalStateException("Minimum object found, but not there when removing");
  }

  @Override
  public synchronized TraceRunnable peek() {
    if (task.isEmpty())
      return null;

    return Collections.min(task, ELEMENT_COMPARATOR);
  }

  @Override
  public synchronized boolean offer(TraceRunnable e) {
    task.add(e);
    notify();
    return true;
  }

  @Override
  public synchronized void put(TraceRunnable e) {
    task.add(e);
    notify();
  }

  @Override
  public synchronized boolean offer(TraceRunnable e, long timeout, TimeUnit unit) {
    task.add(e);
    notify();
    return true;
  }

  @Override
  public synchronized TraceRunnable take() throws InterruptedException {
    while (task.isEmpty()) {
      wait();
    }

    return poll();
  }

  @Override
  public synchronized TraceRunnable poll(long timeout, TimeUnit unit) throws InterruptedException {
    if (task.isEmpty()) {
      wait(unit.toMillis(timeout));
    }

    if (task.isEmpty())
      return null;

    return poll();
  }

  @Override
  public synchronized int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public synchronized int drainTo(Collection<? super TraceRunnable> c) {
    return drainTo(c, task.size());
  }

  @Override
  public synchronized int drainTo(Collection<? super TraceRunnable> c, int maxElements) {
    Collections.sort(task, ELEMENT_COMPARATOR);

    int num = Math.min(task.size(), maxElements);

    Iterator<TraceRunnable> iter = task.iterator();
    for (int i = 0; i < num; i++) {
      c.add(iter.next());
      iter.remove();
    }

    return num;
  }

  @Override
  public synchronized Iterator<TraceRunnable> iterator() {
    Collections.sort(task, ELEMENT_COMPARATOR);

    return task.iterator();
  }

  @Override
  public synchronized int size() {
    return task.size();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  BlockingQueue<Runnable> asBlockingQueueOfRunnable() {
    return (BlockingQueue) this;
  }

}
