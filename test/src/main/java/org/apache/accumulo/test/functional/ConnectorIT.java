package org.apache.accumulo.test.functional;

import java.util.Properties;

import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Assert;
import org.junit.Test;

public class ConnectorIT extends AccumuloClusterHarness {

  @Test
  public void testConnectorBuilder() throws Exception {
    Connector c = getConnector();
    String instanceName = c.getInstance().getInstanceName();
    String zookeepers = c.getInstance().getZooKeepers();

    Connector conn = Connector.builder().forInstance(instanceName, zookeepers)
        .usingCredentials(getAdminPrincipal(), getAdminToken()).build();

    Assert.assertEquals(instanceName, conn.getInstance().getInstanceName());
    Assert.assertEquals(zookeepers, conn.getInstance().getZooKeepers());
    Assert.assertEquals(getAdminPrincipal(), conn.whoami());

    final String user = "testuser";
    final String password = "testpassword";
    conn.securityOperations().createLocalUser(user, new PasswordToken(password));

    Properties props = new Properties();
    props.put(ClientProperty.INSTANCE_NAME.getKey(), instanceName);
    props.put(ClientProperty.INSTANCE_ZK_HOST.getKey(), zookeepers);
    props.put(ClientProperty.USER_NAME.getKey(), user);
    props.put(ClientProperty.USER_PASSWORD.getKey(), password);
    conn = Connector.builder().usingProperties(props).build();

    Assert.assertEquals(instanceName, conn.getInstance().getInstanceName());
    Assert.assertEquals(zookeepers, conn.getInstance().getZooKeepers());
    Assert.assertEquals(user, conn.whoami());
  }
}
