package org.apache.accumulo.server.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class TabletMetadataCache {

  private static final Long INITIAL_VALUE = Long.valueOf(0);
  private final Map<KeyExtent,TabletMetadata> cache = new HashMap<>();
  private final ServerContext ctx;
  private final InstanceId iid;
  private final ZooReaderWriter zrw;

  public TabletMetadataCache(ServerContext ctx) {
    this.ctx = ctx;
    this.iid = this.ctx.getInstanceID();
    this.zrw = this.ctx.getZooReaderWriter();
  }

  public void put(final KeyExtent extent, final TabletMetadata metadata)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    TabletMetadata priorVal = cache.putIfAbsent(extent, metadata);
    if (priorVal == null) {
      final String path = getPath(extent);
      this.zrw.mutateOrCreate(path, serialize(INITIAL_VALUE), (currVal) -> {
        return serialize(deserialize(currVal) + 1);
      });
      watch(path, extent);
    }
  }

  private void watch(final String path, final KeyExtent extent) {

    final Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
          case NodeChildrenChanged:
          case ChildWatchRemoved:
          case PersistentWatchRemoved:
          case DataWatchRemoved:
          case NodeCreated:
            // I don't think we care about these cases, but we need
            // to recreate this watcher
            watch(path, extent);
            break;
          case None:
            Event.KeeperState state = event.getState();
            switch (state) {
              case AuthFailed:
              case Closed:
              case ConnectedReadOnly:
              case Disconnected:
              case Expired:
                // remove all entries on connection issue
                cache.clear();
                // case NoSyncConnected:
                // case SaslAuthenticated:
                // case SyncConnected:
                // case Unknown:
              default:
                // don't care
                break;
            }
          case NodeDataChanged:
          case NodeDeleted:
            remove(extent);
            break;
          default:
            break;
        }
      }
    };
    // Place the watcher
    try {
      this.zrw.exists(path, watcher);
    } catch (KeeperException | InterruptedException e) {
      // TODO: Handle this
    }
  }

  public TabletMetadata get(final KeyExtent extent)
      throws AcceptableThriftTableOperationException, KeeperException, InterruptedException {
    final String path = getPath(extent);
    TabletMetadata val = cache.computeIfAbsent(extent, (k) -> {
      return this.ctx.getAmple().readTablet(k, ColumnType.values());
    });
    this.zrw.mutateOrCreate(path, serialize(INITIAL_VALUE), (currVal) -> {
      return serialize(deserialize(currVal) + 1);
    });
    watch(path, extent);
    return val;
  }

  public void remove(KeyExtent extent) {
    TabletMetadata priorVal = cache.remove(extent);
    if (priorVal != null) {
      try {
        this.zrw.delete(getPath(extent));
      } catch (InterruptedException | KeeperException e) {
        // TODO: handle this
      }
    }
  }

  private String getPath(KeyExtent extent) {
    return Constants.ZROOT + "/" + this.iid + Constants.ZTABLET_CACHE + "/" + extent.toString();
  }

  private Long deserialize(byte[] value) {
    return Long.parseLong(new String(value, UTF_8));
  }

  private byte[] serialize(Long value) {
    return value.toString().getBytes(UTF_8);
  }

  public void tabletMetadataChanged(KeyExtent extent)
      throws AcceptableThriftTableOperationException, KeeperException, InterruptedException {
    this.zrw.mutateOrCreate(getPath(extent), serialize(INITIAL_VALUE), (currVal) -> {
      return serialize(deserialize(currVal) + 1);
    });
  }
}
