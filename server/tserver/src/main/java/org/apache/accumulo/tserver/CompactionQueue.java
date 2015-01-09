/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CompactionQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {

  private List<Comparable> task = new LinkedList<Comparable>();

  @Override
  public synchronized Runnable poll() {
    if (task.size() == 0)
      return null;

    Comparable min = Collections.min(task);
    task.remove(min);
    return (Runnable) min;
  }

  @Override
  public synchronized Runnable peek() {
    if (task.size() == 0)
      return null;

    Comparable min = Collections.min(task);
    return (Runnable) min;
  }

  @Override
  public synchronized boolean offer(Runnable e) {
    task.add((Comparable) e);
    notify();
    return true;
  }

  @Override
  public synchronized void put(Runnable e) throws InterruptedException {
    task.add((Comparable) e);
    notify();
  }

  @Override
  public synchronized boolean offer(Runnable e, long timeout, TimeUnit unit) throws InterruptedException {
    task.add((Comparable) e);
    notify();
    return true;
  }

  @Override
  public synchronized Runnable take() throws InterruptedException {
    while (task.size() == 0) {
      wait();
    }

    return poll();
  }

  @Override
  public synchronized Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
    if (task.size() == 0) {
      wait(unit.toMillis(timeout));
    }

    if (task.size() == 0)
      return null;

    return poll();
  }

  @Override
  public synchronized int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public synchronized int drainTo(Collection<? super Runnable> c) {
    return drainTo(c, task.size());
  }

  @Override
  public synchronized int drainTo(Collection<? super Runnable> c, int maxElements) {
    Collections.sort(task);

    int num = Math.min(task.size(), maxElements);

    Iterator<Comparable> iter = task.iterator();
    for (int i = 0; i < num; i++) {
      c.add((Runnable) iter.next());
      iter.remove();
    }

    return num;
  }

  @Override
  public synchronized Iterator<Runnable> iterator() {
    Collections.sort(task);

    final Iterator<Comparable> iter = task.iterator();

    return new Iterator<Runnable>() {

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Runnable next() {
        return (Runnable) iter.next();
      }

      @Override
      public void remove() {
        iter.remove();
      }
    };
  }

  @Override
  public synchronized int size() {
    return task.size();
  }

}
