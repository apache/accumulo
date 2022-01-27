package org.apache.accumulo.core.spi.scan;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.sample.impl.DataoutputHasher;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DefaultScanServerLocator implements ScanServerLocator {

    private static final long INITIAL_SLEEP_TIME = 100L;
    private static final long MAX_SLEEP_TIME = 300000L;
    private final int INITIAL_SERVERS = 3;
    private final int MAX_DEPTH = 3;

    @Override
    public LocateResult locate(LocateParameters params) {

        // TODO how to handle the case when there are zero scan servers.  Thinking this plugin can make two choices, fall back to the tablet servers serving the tablet or wait and try again later.

        Random rand = new Random();

        Map<TabletId, String> serversMap = new HashMap<>();
        Map<String, Long> sleepTimes = new HashMap<>();

        for (TabletId tablet : params.getTablets()) {
           int hashCode = hashTablet(tablet);

           // TODO handle io errors
           int busyAttempts = (int) params.getScanAttempts().forTablet(tablet).stream().filter(scanAttempt -> scanAttempt.getResult() == ScanAttempt.Result.BUSY).count();

           int numServers;

           if(busyAttempts <MAX_DEPTH) {
               numServers = (int) Math.round(INITIAL_SERVERS * Math.pow(params.getScanServers().size() / (double) INITIAL_SERVERS, busyAttempts / (double)MAX_DEPTH));
           } else {
               numServers = params.getScanServers().size();
           }

            int serverIndex = (hashCode + rand.nextInt(numServers)) % params.getScanServers().size();
            String serverToUse = params.getScanServers().get(serverIndex);

            long sleepTime = 0;
            if(busyAttempts > MAX_DEPTH) {
                sleepTime = INITIAL_SLEEP_TIME * Math.pow(2,busyAttempts - (MAX_DEPTH+1));
                sleepTime = Math.min(sleepTime, MAX_SLEEP_TIME);
            }

            serversMap.put(tablet, serverToUse);
            sleepTimes.merge(serverToUse, sleepTime, Long::max);

        }

        return  new LocateResult() {
            @Override
            public String getServer(TabletId tablet) {
                return serversMap.get(tablet);
            }

            @Override
            public long getSleepTime(String server) {
                return sleepTimes.getOrDefault(server, 0L);
            }
        };
    }

    private int hashTablet(TabletId tablet) {
        var hasher = Hashing.murmur3_32().newHasher();

        hasher.putString(tablet.getTable().canonical(), StandardCharsets.UTF_8);

        if(tablet.getEndRow() != null) {
            hasher.putBytes(tablet.getEndRow().getBytes(), 0 , tablet.getEndRow().getLength());
        }

        if(tablet.getPrevEndRow() != null) {
            hasher.putBytes(tablet.getPrevEndRow().getBytes(), 0 , tablet.getPrevEndRow().getLength());
        }

        return hasher.hash().asInt();
    }
}
