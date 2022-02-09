package org.apache.accumulo.core.spi.scan;

import com.google.common.hash.Hashing;
import org.apache.accumulo.core.data.TabletId;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;

public class DefaultEcScanManager implements EcScanManager {

  private static final long INITIAL_SLEEP_TIME = 100L;
  private static final long MAX_SLEEP_TIME = 300000L;
  private final int INITIAL_SERVERS = 3;
  private final int MAX_DEPTH = 3;

  @Override public EcScanActions determineActions(DaParamaters params) {

    if (params.getScanServers().isEmpty()) {
      return new EcScanActions() {
        @Override public Action getAction(TabletId tablet) {
          return Action.USE_TABLET_SERVER;
        }

        @Override public String getScanServer(TabletId tablet) {
          return null;
        }

        @Override public Duration getDelay(String server) {
          // TODO is delay needed if there were prev errors?
          return Duration.ZERO;
        }
      };
    }
    Random rand = new Random();

    Map<TabletId,String> serversMap = new HashMap<>();
    Map<String,Long> sleepTimes = new HashMap<>();

    for (TabletId tablet : params.getTablets()) {

      SortedSet<ScanAttempt> attempts = params.getScanAttempts().forTablet(tablet);

      long sleepTime = 0;
      String serverToUse;

      if (!attempts.isEmpty() && attempts.last()
          .getResult() == ScanAttempt.Result.SUCCESS && params.getScanServers()
          .contains(attempts.last().getServer())) {
        // Stick with what was chosen last time
        serverToUse = attempts.last().getServer();
      } else {
        int hashCode = hashTablet(tablet);

        // TODO handle io errors
        int busyAttempts = (int) attempts.stream()
            .filter(scanAttempt -> scanAttempt.getResult() == ScanAttempt.Result.BUSY).count();

        int numServers;

        if (busyAttempts < MAX_DEPTH) {
          numServers = (int) Math.round(INITIAL_SERVERS * Math.pow(
              params.getOrderedScanServers().size() / (double) INITIAL_SERVERS,
              busyAttempts / (double) MAX_DEPTH));
        } else {
          numServers = params.getOrderedScanServers().size();
        }

        int serverIndex =
            (hashCode + rand.nextInt(numServers)) % params.getOrderedScanServers().size();
        serverToUse = params.getOrderedScanServers().get(serverIndex);

        if (busyAttempts > MAX_DEPTH) {
          sleepTime = (long) (INITIAL_SLEEP_TIME * Math.pow(2, busyAttempts - (MAX_DEPTH + 1)));
          sleepTime = Math.min(sleepTime, MAX_SLEEP_TIME);
        }
      }
      serversMap.put(tablet, serverToUse);
      sleepTimes.merge(serverToUse, sleepTime, Long::max);

    }

    return new EcScanActions() {
      @Override public Action getAction(TabletId tablet) {
        return Action.USE_SCAN_SERVER;
      }

      @Override public String getScanServer(TabletId tablet) {
        return serversMap.get(tablet);
      }

      @Override public Duration getDelay(String server) {
        return Duration.of(sleepTimes.getOrDefault(server, 0L), ChronoUnit.MILLIS);
      }
    };
  }

  private int hashTablet(TabletId tablet) {
    var hasher = Hashing.murmur3_32().newHasher();

    hasher.putString(tablet.getTable().canonical(), StandardCharsets.UTF_8);

    if (tablet.getEndRow() != null) {
      hasher.putBytes(tablet.getEndRow().getBytes(), 0, tablet.getEndRow().getLength());
    }

    if (tablet.getPrevEndRow() != null) {
      hasher.putBytes(tablet.getPrevEndRow().getBytes(), 0, tablet.getPrevEndRow().getLength());
    }

    return hasher.hash().asInt();
  }
}
