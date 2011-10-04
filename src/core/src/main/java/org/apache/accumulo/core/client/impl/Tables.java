package org.apache.accumulo.core.client.impl;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooUtil;


public class Tables
{

    private static ZooCache getZooCache(Instance instance)
    {
        return ZooCache.getInstance(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    }

    private static SortedMap<String, String> getMap(Instance instance, boolean nameAsKey)
    {
        ZooCache zc = getZooCache(instance);

        List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);

        TreeMap<String, String> tableMap = new TreeMap<String, String>();

        for (String tableId : tableIds) {
            byte[] tblPath = zc.get( ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME );
            if(tblPath != null)
            {
                if (nameAsKey)
                    tableMap.put(new String(tblPath), tableId);
                else
                    tableMap.put(tableId, new String(tblPath));
            }
        }

        return tableMap;
    }

    public static String getTableId(Instance instance, String tableName) throws TableNotFoundException
    {
        String tableId = getNameToIdMap(instance).get(tableName);
        if (tableId == null)
            throw new TableNotFoundException(tableId, tableName, null);
        return tableId;
    }

    public static String getTableName(Instance instance, String tableId) throws TableNotFoundException
    {
        String tableName = getIdToNameMap(instance).get(tableId);
        if (tableName == null)
            throw new TableNotFoundException(tableId, tableName, null);
        return tableName;
    }

    public static SortedMap<String, String> getNameToIdMap(Instance instance)
    {
        return getMap(instance, true);
    }

    public static SortedMap<String, String> getIdToNameMap(Instance instance)
    {
        return getMap(instance, false);
    }

    public static void clearCache(Instance instance)
    {
        getZooCache(instance).clear();
    }

    public static String getPrintableTableNameFromId(Map<String, String> tidToNameMap, String tableId)
    {
        String tableName = tidToNameMap.get(tableId);
        return tableName == null ? "(ID:" + tableId + ")" : tableName;
    }

    public static String getPrintableTableIdFromName(Map<String, String> nameToIdMap, String tableName)
    {
        String tableId = nameToIdMap.get(tableName);
        return tableId == null ? "(NAME:" + tableName + ")" : tableId;
    }
}
