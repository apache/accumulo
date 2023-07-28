/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util.collections;

import static java.util.Objects.requireNonNull;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

public class WeighingBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

  // This class exist so that objects are only weighed once to defend against getting different
  // weights are different times when calling the weighing function.
  private static class Weighed<T> {
    final T obj;
    final int weight;

    Weighed(T obj, int weight) {
      this.obj = obj;
      this.weight = weight;
    }
  }

  private final Semaphore totalWeight;
  private final LinkedBlockingQueue<Weighed<T>> queue = new LinkedBlockingQueue<>();

  private final ToIntFunction<T> weigher;

  public WeighingBlockingQueue(int maxWeight, ToIntFunction<T> weigher) {
    Preconditions.checkArgument(maxWeight > 0);
    requireNonNull(weigher);
    this.totalWeight = new Semaphore(maxWeight);
    this.weigher = obj -> weigher.applyAsInt(requireNonNull(obj));
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.transform(queue.iterator(), weighed -> weighed.obj);
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public void put(T t) throws InterruptedException {
    int weight = weigher.applyAsInt(t);
    totalWeight.acquire(weight);
    queue.put(new Weighed<>(t, weight));
  }

  @Override
  public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
    int weight = weigher.applyAsInt(t);
    if (totalWeight.tryAcquire(weight, timeout, unit)) {
      queue.offer(new Weighed<T>(t, weight));
      return true;
    }
    return false;
  }

  private T release(Weighed<T> weighed) {
    if (weighed == null) {
      return null;
    }

    totalWeight.release(weighed.weight);
    return weighed.obj;
  }

  @Override
  public T take() throws InterruptedException {
    return release(queue.take());
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    return release(queue.poll(timeout, unit));

  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    // TODO would be nice to avoid this buffer and directly add
    ArrayList<Weighed<T>> buffer = new ArrayList<>();
    queue.drainTo(buffer);

    // Release before before adding to collection in case adding to collection throws an exception.
    // These elements are already out of the queue so want to release even if there is an exception.
    buffer.forEach(this::release);
    buffer.forEach(weighed -> c.add(weighed.obj));
    return buffer.size();
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    ArrayList<Weighed<T>> buffer = new ArrayList<>();
    queue.drainTo(buffer, maxElements);

    // Release before before adding to collection in case adding to collection throws an exception.
    // These elements are already out of the queue so want to release even if there is an exception.
    buffer.forEach(this::release);
    buffer.forEach(weighed -> c.add(weighed.obj));
    return buffer.size();
  }

  @Override
  public boolean offer(T t) {
    int weight = weigher.applyAsInt(t);
    if (totalWeight.tryAcquire(weight)) {
      queue.offer(new Weighed<>(t, weight));
      return true;
    }
    return false;
  }

  @Override
  public T poll() {
    return release(queue.poll());
  }

  @Override
  public T peek() {
    Weighed<T> weighed = queue.peek();
    if (weighed == null) {
      return null;
    }
    return weighed.obj;
  }
}
