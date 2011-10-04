package org.apache.accumulo.server.util;

import java.util.Arrays;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


public class TableDiskUsage {
    
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        
        Instance instance = HdfsZooInstance.getInstance();
        Connector conn = instance.getConnector("root", "secret");
        
        org.apache.accumulo.core.util.TableDiskUsage.printDiskUsage(ServerConfiguration.getSystemConfiguration(), Arrays.asList(args), fs, conn);
    }

}
