package org.apache.accumulo.server;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.fs.Path;

import static org.apache.accumulo.core.Constants.*;

public class ServerConstants {
    // these are functions to delay loading the Accumulo configuration unless we must
    public static String getBaseDir() {
        return ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_DIR);
    }
    public static String getTablesDir() {
        return getBaseDir() + "/tables";
    }
    public static String getRecoveryDir() {
        return getBaseDir() + "/recovery";
    }
    public static Path getInstanceIdLocation() {
        return new Path(getBaseDir() + "/instance_id");
    }
    public static Path getDataVersionLocation() {
        return new Path(getBaseDir() + "/version");
    }
    public static String getMetadataTableDir() {
        return getTablesDir() + "/" + METADATA_TABLE_ID;
    }
    public static String getRootTabletDir() {
        return getMetadataTableDir()+ZROOT_TABLET;
    }

}
