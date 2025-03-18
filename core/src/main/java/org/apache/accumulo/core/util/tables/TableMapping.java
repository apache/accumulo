package org.apache.accumulo.core.util.tables;

import static java.util.Collections.emptySortedMap;
import static org.apache.accumulo.core.clientImpl.NamespaceMapping.deserialize;

import java.util.Map;
import java.util.SortedMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;

import com.google.common.collect.ImmutableSortedMap;

public class TableMapping {

  // type token must represent a mutable type, so it can be altered in the mutateExisting methods
  // without needing to make a copy
  private final ClientContext context;
  private final NamespaceId namespaceId;
  private volatile SortedMap<TableId,String> currentTableMap = emptySortedMap();
  private volatile SortedMap<String,TableId> currentTableReverseMap = emptySortedMap();
  private volatile long lastMzxid;

  public TableMapping(ClientContext context, NamespaceId namespaceId) {
    this.context = context;
    this.namespaceId = namespaceId;
  }

  private synchronized void update(NamespaceId namespaceId) {
    final ZooCache zc = context.getZooCache();
    final ZcStat stat = new ZcStat();

    byte[] data = zc.get(Constants.ZNAMESPACES + "/" + namespaceId, stat);
    if (stat.getMzxid() > lastMzxid) {
      if (data == null) {
        throw new IllegalStateException(
            Constants.ZNAMESPACES + "/" + namespaceId + " node should not be null");
      } else {
        Map<String,String> idToName = deserialize(data);
        if (namespaceId == Namespace.ACCUMULO.id()
            && (!idToName.containsKey(AccumuloTable.ROOT.tableId().canonical())
                || !idToName.containsKey(AccumuloTable.METADATA.tableId().canonical()))) {
          throw new IllegalStateException("Built-in tables are not present in map");
        }
        var converted = ImmutableSortedMap.<TableId,String>naturalOrder();
        var convertedReverse = ImmutableSortedMap.<String,TableId>naturalOrder();
        idToName.forEach((idString, name) -> {
          var id = TableId.of(idString);
          converted.put(id, name);
          convertedReverse.put(name, id);
        });
        currentTableMap = converted.build();
        currentTableReverseMap = convertedReverse.build();
      }
      lastMzxid = stat.getMzxid();
    }
  }

  public SortedMap<TableId,String> getIdToNameMap(NamespaceId namespaceId) {
    update(namespaceId);
    return currentTableMap;
  }

  public SortedMap<String,TableId> getNameToIdMap(NamespaceId namespaceId) {
    update(namespaceId);
    return currentTableReverseMap;
  }

}
