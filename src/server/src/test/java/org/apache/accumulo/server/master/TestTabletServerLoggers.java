package org.apache.accumulo.server.master;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.server.master.TabletServerLoggers;
import org.apache.accumulo.server.master.TabletServerLoggers.NewLoggerWatcher;
import org.junit.Test;


public class TestTabletServerLoggers {
    
    String[] theList;
    
    static class Watcher implements NewLoggerWatcher {
        public ArrayList<String> results = new ArrayList<String>();

        @Override
        public void newLogger(String address) {
            results.add(address);
        } 
    }
    Watcher watcher = new Watcher();
    
    class TabletServerLoggers_ extends TabletServerLoggers {
        
        public String[] theList = new String[0];
        
        
        TabletServerLoggers_() {
            super(watcher);
        }
      
        @Override
        public synchronized Map<String, String> getLoggersFromZooKeeper() {
            HashMap<String, String> result = new HashMap<String, String>();
            for (int i = 0; i < theList.length; i++) {
                result.put("" + i, theList[i]);
            }
            return result;
        }

        
    }

    @Test
    public void testScanZooKeeperForUpdates() throws Exception {
        String[] loggers = 
        {
         "1.2.3.4:1234",
         "1.1.1.2:1234",
         "1.1.1.3:1234",
        };
        TabletServerLoggers_ lgs = new TabletServerLoggers_();
        lgs.scanZooKeeperForUpdates();
        assertEquals(lgs.getLoggersFromZooKeeper().size(), 0);
        lgs.theList = loggers;
        lgs.scanZooKeeperForUpdates();
        assertEquals(lgs.getLoggersFromZooKeeper().size(), 3);
        String[] update = 
        {
         "1.2.3.4:1234"
        };
        lgs.theList = update;
        lgs.scanZooKeeperForUpdates();
        assertEquals(lgs.getLoggersFromZooKeeper().size(), 1);
    }

    @Test
    public void testLeastBusyLoggers() throws Exception {
        String[] loggers = 
        {
         "1.2.3.4:1234",
         "1.1.1.2:1234",
         "1.1.1.3:1234",
        };
        TabletServerLoggers_ lgs = new TabletServerLoggers_();
        lgs.theList = loggers;
        lgs.scanZooKeeperForUpdates();
        assertEquals(lgs.getLoggersFromZooKeeper().size(), 3);
        lgs.assignLoggersToTabletServer(Arrays.asList("1.2.3.4:1234".split(",")), "1.2.3.1:4567");
        lgs.assignLoggersToTabletServer(Arrays.asList("1.2.3.4:1234,1.1.1.2:1234".split(",")), "1.2.3.2:4567");
        lgs.assignLoggersToTabletServer(Arrays.asList("1.2.3.4:1234,1.1.1.2:1234,1.1.1.3:1234".split(",")), "1.2.3.3:4567");
        List<String> sort = lgs.leastBusyLoggers();
        assertEquals(sort.size(), 3);
        assertEquals(sort.get(0), "1.1.1.3:1234");
        assertEquals(sort.get(1), "1.1.1.2:1234");
        assertEquals(sort.get(2), "1.2.3.4:1234");
        
    }

}
