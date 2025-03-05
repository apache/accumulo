package org.apache.accumulo.minicluster;

import java.io.File;

import org.junit.jupiter.api.Test;

public class MiniAccumuloMiniAccumuloServerConfigTest {

  // example of starting a mini cluster with multiple resource groups

  @Test
  public void testServerConfig() throws Exception {
    var builder = MiniAccumuloServerConfig.builder().putDefaults();
    builder.putTabletServerResourceGroup("TS1", 2);
    builder.putScanServerResourceGroup("SG1", 2);
    builder.putCompactorResourceGroup("CG1", 2);
    builder.putCompactorResourceGroup("CG2", 2);

    var serverConfig = builder.build();

    File dir = null; // TODO
    MiniAccumuloConfig miniCfg = new MiniAccumuloConfig(dir, "abc123");
    miniCfg.setServerConfig(serverConfig);

    var cluster = new MiniAccumuloCluster(miniCfg);
    cluster.start();
  }

}
