package org.apache.accumulo.server.util;

import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


/**
 * Remove file entries for map files that don't exist.
 *
 */
public class RemoveEntriesForMissingFiles {
    private static Logger log = Logger.getLogger(RemoveEntriesForMissingFiles.class);

    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
        if (args.length < 4) {
            System.err.println("Usage: accumulo.server.util.RemoveEntriesForMissingFiles instance zookeepers username password [delete]");
            System.exit(1);
        }
        Instance instance = new ZooKeeperInstance(args[0], args[1]);
        Connector connector = instance.getConnector(args[2], args[3].getBytes());
        Scanner metadata = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        metadata.setBatchSize(1000*1000);
        metadata.setRange(Constants.METADATA_KEYSPACE);
        metadata.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
        int count = 0;
        int missing = 0;
        Writer writer = MetadataTable.getMetadataTable(SecurityConstants.systemCredentials);
        for (Entry<Key, Value> entry : metadata)
        {
            count++;
            Key key = entry.getKey();
            String table = new String(KeyExtent.tableOfMetadataRow(entry.getKey().getRow()));
            Path map = new Path(Constants.getTablesDir() + "/" + 
                                table +
                                key.getColumnQualifier().toString());
            if (!fs.exists(map)) {
                missing++;
                log.info("File " + map + " is missing");
                Mutation m = new Mutation(key.getRow());
                m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
                if (args.length > 4) {
                    writer.update(m);
                    log.info("entry removed from metadata table: " + m);
                }
            }
        }
        log.info(String.format("%d files of %d missing", missing, count));
    }
}
