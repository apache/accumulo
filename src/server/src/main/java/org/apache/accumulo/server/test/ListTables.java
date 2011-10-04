package org.apache.accumulo.server.test;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.Tables;


/**
 * This little program is used by the functional test to get
 * a list of table ids.
 */
public class ListTables
{
    public static void main(String[] args)
    {
        for (Entry<String, String> table : Tables.getNameToIdMap(HdfsZooInstance.getInstance()).entrySet())
            System.out.println(table.getKey() + " => " + table.getValue());
    }
}
