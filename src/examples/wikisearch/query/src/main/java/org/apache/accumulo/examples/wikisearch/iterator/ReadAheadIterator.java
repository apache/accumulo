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
package org.apache.accumulo.examples.wikisearch.iterator;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This iterator takes the source iterator (the one below it in the iterator stack) and puts it in a background thread. The background thread continues
 * processing and fills a queue with the Keys and Values from the source iterator. When seek() is called on this iterator, it pauses the background thread,
 * clears the queue, calls seek() on the source iterator, then resumes the thread filling the queue.
 * 
 * Users of this iterator can set the queue size, default is five elements. Users must be aware of the potential for OutOfMemory errors when using this iterator
 * with large queue sizes or large objects. This iterator copies the Key and Value from the source iterator and puts them into the queue.
 * 
 * This iterator introduces some parallelism into the server side iterator stack. One use case for this would be when an iterator takes a relatively long time
 * to process each K,V pair and causes the iterators above it to wait. By putting the longer running iterator in a background thread we should be able to
 * achieve greater throughput.
 * 
 * NOTE: Experimental!
 * 
 */
public class ReadAheadIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
  
  private static Logger log = Logger.getLogger(ReadAheadIterator.class);
  
  public static final String QUEUE_SIZE = "queue.size";
  
  public static final String TIMEOUT = "timeout";
  
  private static final QueueElement noMoreDataElement = new QueueElement();
  
  private int queueSize = 5;
  
  private int timeout = 60;
  
  /**
   * 
   * Class to hold key and value from the producing thread.
   * 
   */
  static class QueueElement {
    Key key = null;
    Value value = null;
    
    public QueueElement() {}
    
    public QueueElement(Key key, Value value) {
      super();
      this.key = new Key(key);
      this.value = new Value(value.get(), true);
    }
    
    public Key getKey() {
      return key;
    }
    
    public Value getValue() {
      return value;
    }
  }
  
  /**
   * 
   * Thread that produces data from the source iterator and places the results in a queue.
   * 
   */
  class ProducerThread extends ReentrantLock implements Runnable {
    
    private static final long serialVersionUID = 1L;
    
    private Exception e = null;
    
    private int waitTime = timeout;
    
    private SortedKeyValueIterator<Key,Value> sourceIter = null;
    
    public ProducerThread(SortedKeyValueIterator<Key,Value> source) {
      this.sourceIter = source;
    }
    
    public void run() {
      boolean hasMoreData = true;
      // Keep this thread running while there is more data to read
      // and items left in the queue to be read off.
      while (hasMoreData || queue.size() > 0) {
        try {
          // Acquire the lock, this will wait if the lock is being
          // held by the ReadAheadIterator.seek() method.
          this.lock();
          // Check to see if there is more data from the iterator below.
          hasMoreData = sourceIter.hasTop();
          // Break out of the loop if no more data.
          if (!hasMoreData)
            continue;
          // Put the next K,V onto the queue.
          try {
            QueueElement e = new QueueElement(sourceIter.getTopKey(), sourceIter.getTopValue());
            boolean inserted = false;
            try {
              inserted = queue.offer(e, this.waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
              this.e = ie;
              break;
            }
            if (!inserted) {
              // Then we either got a timeout, set the error and break out of the loop
              this.e = new TimeoutException("Background thread has exceeded wait time of " + this.waitTime + " seconds, aborting...");
              break;
            }
            // Move the iterator to the next K,V for the next iteration of this loop
            sourceIter.next();
          } catch (Exception e) {
            this.e = e;
            log.error("Error calling next on source iterator", e);
            break;
          }
        } finally {
          this.unlock();
        }
      }
      // If we broke out of the loop because of an error, then don't put the marker on the queue, just to do end.
      if (!hasError()) {
        // Put the special end of data marker into the queue
        try {
          queue.put(noMoreDataElement);
        } catch (InterruptedException e) {
          this.e = e;
          log.error("Error putting End of Data marker onto queue");
        }
      }
    }
    
    public boolean hasError() {
      return (this.e != null);
    }
    
    public Exception getError() {
      return this.e;
    }
  }
  
  private SortedKeyValueIterator<Key,Value> source;
  private ArrayBlockingQueue<QueueElement> queue = null;
  private QueueElement currentElement = new QueueElement();
  private ProducerThread thread = null;
  private Thread t = null;
  
  protected ReadAheadIterator(ReadAheadIterator other, IteratorEnvironment env) {
    source = other.source.deepCopy(env);
  }
  
  public ReadAheadIterator() {}
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new ReadAheadIterator(this, env);
  }
  
  public Key getTopKey() {
    return currentElement.getKey();
  }
  
  public Value getTopValue() {
    return currentElement.getValue();
  }
  
  public boolean hasTop() {
    if (currentElement == noMoreDataElement)
      return false;
    return currentElement != null || queue.size() > 0 || source.hasTop();
  }
  
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    validateOptions(options);
    this.source = source;
    queue = new ArrayBlockingQueue<QueueElement>(queueSize);
    thread = new ProducerThread(this.source);
    t = new Thread(thread, "ReadAheadIterator-SourceThread");
    t.start();
  }
  
  /**
   * Populate the key and value
   */
  public void next() throws IOException {
    // Thread startup race condition, need to make sure that the
    // thread has started before we call this the first time.
    while (t.getState().equals(State.NEW)) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {}
    }
    
    if (t.getState().equals(State.TERMINATED)) {
      // Thread encountered an error.
      if (thread.hasError()) {
        // and it should
        throw new IOException("Background thread has died", thread.getError());
      }
    }
    
    // Pull an element off the queue, this will wait if there is no data yet.
    try {
      if (thread.hasError())
        throw new IOException("background thread has error", thread.getError());
      
      QueueElement nextElement = null;
      while (null == nextElement) {
        try {
          nextElement = queue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // TODO: Do we need to do anything here?
        }
        if (null == nextElement) {
          // Then we have no data and timed out, check for error condition in the read ahead thread
          if (thread.hasError()) {
            throw new IOException("background thread has error", thread.getError());
          }
        }
      }
      currentElement = nextElement;
    } catch (IOException e) {
      throw new IOException("Error getting element from source iterator", e);
    }
  }
  
  /**
   * Seek to the next matching cell and call next to populate the key and value.
   */
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (t.isAlive()) {
      // Check for error
      if (thread.hasError())
        throw new IOException("background thread has error", thread.getError());
      
      try {
        // Acquire the lock, or wait until its unlocked by the producer thread.
        thread.lock();
        queue.clear();
        currentElement = null;
        source.seek(range, columnFamilies, inclusive);
      } finally {
        thread.unlock();
      }
      next();
    } else {
      throw new IOException("source iterator thread has died.");
    }
  }
  
  public IteratorOptions describeOptions() {
    Map<String,String> options = new HashMap<String,String>();
    options.put(QUEUE_SIZE, "read ahead queue size");
    options.put(TIMEOUT, "timeout in seconds before background thread thinks that the client has aborted");
    return new IteratorOptions(getClass().getSimpleName(), "Iterator that puts the source in another thread", options, null);
  }
  
  public boolean validateOptions(Map<String,String> options) {
    if (options.containsKey(QUEUE_SIZE))
      queueSize = Integer.parseInt(options.get(QUEUE_SIZE));
    if (options.containsKey(TIMEOUT))
      timeout = Integer.parseInt(options.get(TIMEOUT));
    return true;
  }
  
}
