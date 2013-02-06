package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.Properties;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;

public class Config extends Test {
  
  private static final String LAST_SETTING = "lastSetting";


  static class Setting {
    public Property property;
    public long min;
    public long max;
    public Setting(Property property, long min, long max) {
      this.property = property;
      this.min = min;
      this.max = max;
    }
  }
  Setting[] settings = {
      new Setting(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, 1, 10),
      new Setting(Property.TSERV_BULK_PROCESS_THREADS, 1, 10),
      new Setting(Property.TSERV_BULK_RETRY, 1, 10),
      new Setting(Property.TSERV_BULK_TIMEOUT, 10, 600),
      new Setting(Property.TSERV_BULK_ASSIGNMENT_THREADS, 1, 10),
      new Setting(Property.TSERV_DATACACHE_SIZE, 0, 1000000000L),
      new Setting(Property.TSERV_INDEXCACHE_SIZE, 0, 1000000000L),
      new Setting(Property.TSERV_CLIENT_TIMEOUT, 100, 10000),
      new Setting(Property.TSERV_MAJC_MAXCONCURRENT, 1, 10),
      new Setting(Property.TSERV_MAJC_DELAY, 100, 10000),
      new Setting(Property.TSERV_MAJC_THREAD_MAXOPEN, 3, 100),
      new Setting(Property.TSERV_MINC_MAXCONCURRENT, 1, 10),
      new Setting(Property.TSERV_DEFAULT_BLOCKSIZE, 100000, 10000000L),
      new Setting(Property.TSERV_MAX_IDLE, 10000, 500*1000),
      new Setting(Property.TSERV_MAXMEM, 1000000, 3*1024*1024*1024L),
      new Setting(Property.TSERV_READ_AHEAD_MAXCONCURRENT, 1, 25),
      new Setting(Property.TSERV_MIGRATE_MAXCONCURRENT, 1, 10),
      new Setting(Property.TSERV_MUTATION_QUEUE_MAX, 10000, 1024*1024),
      new Setting(Property.TSERV_RECOVERY_MAX_CONCURRENT, 1, 100),
      new Setting(Property.TSERV_SCAN_MAX_OPENFILES, 10, 1000),
      new Setting(Property.TSERV_THREADCHECK, 100, 10000),
      new Setting(Property.TSERV_MINTHREADS, 1, 100),
      new Setting(Property.TSERV_SESSION_MAXIDLE, 100, 5*60*1000),
      new Setting(Property.TSERV_SORT_BUFFER_SIZE, 1024*1024, 1024*1024*1024L),
      new Setting(Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN, 5, 100),
      new Setting(Property.TSERV_WAL_BLOCKSIZE, 100*1024, 1024*1024*1024*10L),
      new Setting(Property.TSERV_WORKQ_THREADS, 1, 10),
      new Setting(Property.MASTER_BULK_THREADPOOL_SIZE, 1, 10),
      new Setting(Property.MASTER_BULK_RETRIES, 1, 10),
      new Setting(Property.MASTER_BULK_TIMEOUT, 10, 600),
      new Setting(Property.MASTER_FATE_THREADPOOL_SIZE, 1, 100),
      new Setting(Property.MASTER_RECOVERY_DELAY, 0, 10000),
      new Setting(Property.MASTER_RECOVERY_MAXTIME, 10000, 1000000),
      new Setting(Property.MASTER_THREADCHECK, 100, 10000),
      new Setting(Property.MASTER_MINTHREADS, 1, 200),
  };
  
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    // reset any previous setting
    Object lastSetting = state.getMap().get(LAST_SETTING);
    if (lastSetting != null) {
      int choice = Integer.parseInt(lastSetting.toString());
      Property property = settings[choice].property;
      log.debug("Setting " + property.getKey() + " back to " + property.getDefaultValue());
      state.getConnector().instanceOperations().setProperty(property.getKey(), property.getDefaultValue());
    }
    
    // pick a random property
    RandomData random = new RandomDataImpl();
    int choice = random.nextInt(0, settings.length - 1);
    Setting setting = settings[choice];
    // generate a random value
    long newValue = random.nextLong(setting.min, setting.max);
    state.getMap().put(LAST_SETTING, "" + choice);
    log.debug("Setting " + setting.property.getKey() + " to " + newValue);
    state.getConnector().instanceOperations().setProperty(setting.property.getKey(), ""+newValue);
  }
  
}
