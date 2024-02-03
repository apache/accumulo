package org.apache.accumulo.minicluster;

import org.junit.jupiter.api.Test;

import java.io.File;

public class MiniAccumuloResourceGroupsTest {

    // example of starting a mini cluster with multiple resource groups

    @Test
    public void testResourceGroups() throws Exception {
        var builder = ResourceGroups.builder().putDefaults();
        builder.put(ServerType.COMPACTOR, "CG1", 2);
        builder.put(ServerType.COMPACTOR, "CG2", 2);
        builder.put(ServerType.TABLET_SERVER, "TS1", 2);
        builder.put(ServerType.SCAN_SERVER, "SG1", 2);
        var resourceGroups = builder.build();

        File dir  = null; //TODO
        MiniAccumuloConfig miniCfg = new MiniAccumuloConfig(dir, "abc123");
        miniCfg.setResourceGroups(resourceGroups);

        var cluster = new MiniAccumuloCluster(miniCfg);
        cluster.start();
    }



}
