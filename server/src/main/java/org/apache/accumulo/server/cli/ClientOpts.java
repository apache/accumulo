package org.apache.accumulo.server.cli;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.server.client.HdfsZooInstance;

public class ClientOpts extends org.apache.accumulo.core.cli.ClientOpts {
  
  {
    user = "root";
  }

  @Override
  public Instance getInstance() {
    if (mock)
      return new MockInstance(instance);
    if (instance == null) {
      return HdfsZooInstance.getInstance();
    }
    return new ZooKeeperInstance(this.instance, this.zookeepers);
  }
}
