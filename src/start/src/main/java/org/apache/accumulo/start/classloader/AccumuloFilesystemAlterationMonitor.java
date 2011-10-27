/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.start.classloader;

//package org.apache.commons.jci.monitor;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.jci.monitor.FilesystemAlterationListener;
import org.apache.commons.jci.monitor.FilesystemAlterationObserver;
import org.apache.commons.jci.monitor.FilesystemAlterationObserverImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * It's a runnable that spawns of a monitoring thread triggering the the observers and managing the their listeners.
 * 
 * @author tcurdt
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class AccumuloFilesystemAlterationMonitor implements Runnable {
  
  private final Log log = LogFactory.getLog(AccumuloFilesystemAlterationMonitor.class);
  
  private final Object observersLock = new Object();
  private Map observers = Collections.unmodifiableMap(new HashMap());
  private long delay = 3000;
  private Thread thread;
  
  private volatile boolean running = true;
  
  public AccumuloFilesystemAlterationMonitor() {}
  
  public void start() {
    thread = new Thread(this, this.getClass().getName());
    thread.setDaemon(true);
    thread.start();
  }
  
  public void stop() {
    running = false;
    
    try {
      thread.interrupt();
      thread.join(10);
    } catch (InterruptedException e) {}
  }
  
  public void setInterval(final long pDelay) {
    delay = pDelay;
  }
  
  public void addListener(final File pRoot, final FilesystemAlterationListener pListener) {
    
    FilesystemAlterationObserver observer;
    
    synchronized (observersLock) {
      observer = (FilesystemAlterationObserver) observers.get(pRoot);
      
      if (observer == null) {
        final Map newObservers = new HashMap(observers);
        observer = new FilesystemAlterationObserverImpl(pRoot);
        newObservers.put(pRoot, observer);
        observers = Collections.unmodifiableMap(newObservers);
      }
    }
    
    observer.addListener(pListener);
  }
  
  public void removeListener(final FilesystemAlterationListener pListener) {
    synchronized (observersLock) {
      for (Iterator it = observers.values().iterator(); it.hasNext();) {
        final FilesystemAlterationObserver observer = (FilesystemAlterationObserver) it.next();
        observer.removeListener(pListener);
        // FIXME: remove observer if there are no listeners?
      }
    }
  }
  
  public FilesystemAlterationListener[] getListenersFor(final File pRoot) {
    final FilesystemAlterationObserver observer = (FilesystemAlterationObserver) observers.get(pRoot);
    
    if (observer == null) {
      return new FilesystemAlterationListener[0];
    }
    
    return observer.getListeners();
  }
  
  public void run() {
    log.debug("fam running");
    
    while (true) {
      if (!running) {
        break;
      }
      
      final Map currentObservers = observers;
      
      for (Iterator it = currentObservers.values().iterator(); it.hasNext();) {
        final FilesystemAlterationObserver observer = (FilesystemAlterationObserver) it.next();
        observer.checkAndNotify();
      }
      
      if (!running)
        break;
      try {
        Thread.sleep(delay);
      } catch (final InterruptedException e) {}
    }
    
    log.debug("fam exiting");
  }
  
}
