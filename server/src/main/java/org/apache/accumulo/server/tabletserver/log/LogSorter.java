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
package org.apache.accumulo.server.tabletserver.log;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.security.crypto.CryptoModuleFactory;
import org.apache.accumulo.core.security.crypto.CryptoModuleParameters;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue.Processor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * 
 */
public class LogSorter {
  
  private static final Logger log = Logger.getLogger(LogSorter.class);
  VolumeManager fs;
  AccumuloConfiguration conf;
  
  private final Map<String,LogProcessor> currentWork = Collections.synchronizedMap(new HashMap<String,LogProcessor>());
  
  class LogProcessor implements Processor {
    
    private FSDataInputStream input;
    private DataInputStream decryptingInput;
    private long bytesCopied = -1;
    private long sortStart = 0;
    private long sortStop = -1;
    
    @Override
    public Processor newProcessor() {
      return new LogProcessor();
    }
    
    @Override
    public void process(String child, byte[] data) {
      String work = new String(data);
      String[] parts = work.split("\\|");
      String src = parts[0];
      String dest = parts[1];
      String sortId = new Path(src).getName();
      log.debug("Sorting " + src + " to " + dest + " using sortId " + sortId);
      
      synchronized (currentWork) {
        if (currentWork.containsKey(sortId))
          return;
        currentWork.put(sortId, this);
      }
      
      try {
        log.info("Copying " + src + " to " + dest);
        sort(sortId, new Path(src), dest);
      } finally {
        currentWork.remove(sortId);
      }
      
    }
    
    public void sort(String name, Path srcPath, String destPath) {
      
      synchronized (this) {
        sortStart = System.currentTimeMillis();
      }
      
      String formerThreadName = Thread.currentThread().getName();
      int part = 0;
      try {
        
        // the following call does not throw an exception if the file/dir does not exist
        fs.deleteRecursively(new Path(destPath));
        
        FSDataInputStream tmpInput = fs.open(srcPath);
                
        byte[] magic = DfsLogger.LOG_FILE_HEADER_V2.getBytes();
        byte[] magicBuffer = new byte[magic.length];
        tmpInput.readFully(magicBuffer);
        if (!Arrays.equals(magicBuffer, magic)) {
          tmpInput.seek(0);
          synchronized (this) {
           this.input = tmpInput;
           this.decryptingInput = tmpInput;
          }
        } else {
          // We read the crypto module class name here because we need to boot strap the class.  The class itself will read any 
          // additional parameters it needs from the underlying stream.
          String cryptoModuleClassname = tmpInput.readUTF();
          org.apache.accumulo.core.security.crypto.CryptoModule cryptoModule = org.apache.accumulo.core.security.crypto.CryptoModuleFactory
              .getCryptoModule(cryptoModuleClassname);
          
          // Create the parameters and set the input stream into those parameters
          CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
          params.setEncryptedInputStream(tmpInput);
          
          // Create the plaintext input stream from the encrypted one
          params = cryptoModule.getDecryptingInputStream(params);
          
          // Store the plaintext input stream into member variables
          synchronized (this) {
            this.input = tmpInput;
            
            if (params.getPlaintextInputStream() instanceof DataInputStream) {
              this.decryptingInput = (DataInputStream)params.getPlaintextInputStream();              
            } else {
              this.decryptingInput = new DataInputStream(params.getPlaintextInputStream());
            }
            
          }
          
        }
                
        
        final long bufferSize = conf.getMemoryInBytes(Property.TSERV_SORT_BUFFER_SIZE);
        Thread.currentThread().setName("Sorting " + name + " for recovery");
        while (true) {
          final ArrayList<Pair<LogFileKey,LogFileValue>> buffer = new ArrayList<Pair<LogFileKey,LogFileValue>>();
          try {
            long start = input.getPos();
            while (input.getPos() - start < bufferSize) {
              LogFileKey key = new LogFileKey();
              LogFileValue value = new LogFileValue();
              key.readFields(decryptingInput);
              value.readFields(decryptingInput);
              buffer.add(new Pair<LogFileKey,LogFileValue>(key, value));
            }
            writeBuffer(destPath, buffer, part++);
            buffer.clear();
          } catch (EOFException ex) {
            writeBuffer(destPath, buffer, part++);
            break;
          }
        }
        fs.create(new Path(destPath, "finished")).close();
        log.info("Finished log sort " + name + " " + getBytesCopied() + " bytes " + part + " parts in " + getSortTime() + "ms");
      } catch (Throwable t) {
        try {
          // parent dir may not exist
          fs.mkdirs(new Path(destPath));
          fs.create(new Path(destPath, "failed")).close();
        } catch (IOException e) {
          log.error("Error creating failed flag file " + name, e);
        }
        log.error(t, t);
      } finally {
        Thread.currentThread().setName(formerThreadName);
        try {
          close();
        } catch (Exception e) {
          log.error("Error during cleanup sort/copy " + name, e);
        }
        synchronized (this) {
          sortStop = System.currentTimeMillis();
        }
      }
    }
    
    private void writeBuffer(String destPath, ArrayList<Pair<LogFileKey,LogFileValue>> buffer, int part) throws IOException {
      Path path = new Path(destPath, String.format("part-r-%05d", part++));
      FileSystem ns = fs.getFileSystemByPath(path);
      MapFile.Writer output = new MapFile.Writer(ns.getConf(), ns, path.toString(), LogFileKey.class, LogFileValue.class);
      try {
        Collections.sort(buffer, new Comparator<Pair<LogFileKey,LogFileValue>>() {
          @Override
          public int compare(Pair<LogFileKey,LogFileValue> o1, Pair<LogFileKey,LogFileValue> o2) {
            return o1.getFirst().compareTo(o2.getFirst());
          }
        });
        for (Pair<LogFileKey,LogFileValue> entry : buffer) {
          output.append(entry.getFirst(), entry.getSecond());
        }
      } finally {
        output.close();
      }
    }
    
    synchronized void close() throws IOException {
      bytesCopied = input.getPos();
      input.close();
      decryptingInput.close();
      input = null;
    }
    
    public synchronized long getSortTime() {
      if (sortStart > 0) {
        if (sortStop > 0)
          return sortStop - sortStart;
        return System.currentTimeMillis() - sortStart;
      }
      return 0;
    }
    
    synchronized long getBytesCopied() throws IOException {
      return input == null ? bytesCopied : input.getPos();
    }
  }
  
  ThreadPoolExecutor threadPool;
  private final Instance instance;
  
  public LogSorter(Instance instance, VolumeManager fs, AccumuloConfiguration conf) {
    this.instance = instance;
    this.fs = fs;
    this.conf = conf;
    int threadPoolSize = conf.getCount(Property.TSERV_RECOVERY_MAX_CONCURRENT);
    this.threadPool = new SimpleThreadPool(threadPoolSize, this.getClass().getName());
  }
  
  public void startWatchingForRecoveryLogs(ThreadPoolExecutor distWorkQThreadPool) throws KeeperException, InterruptedException {
    this.threadPool = distWorkQThreadPool;
    new DistributedWorkQueue(ZooUtil.getRoot(instance) + Constants.ZRECOVERY).startProcessing(new LogProcessor(), this.threadPool);
  }
  
  public List<RecoveryStatus> getLogSorts() {
    List<RecoveryStatus> result = new ArrayList<RecoveryStatus>();
    synchronized (currentWork) {
      for (Entry<String,LogProcessor> entries : currentWork.entrySet()) {
        RecoveryStatus status = new RecoveryStatus();
        status.name = entries.getKey();
        try {
          status.progress = entries.getValue().getBytesCopied() / (0.0 + conf.getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE));
        } catch (IOException ex) {
          log.warn("Error getting bytes read");
        }
        status.runtime = (int) entries.getValue().getSortTime();
        result.add(status);
      }
      return result;
    }
  }
}
