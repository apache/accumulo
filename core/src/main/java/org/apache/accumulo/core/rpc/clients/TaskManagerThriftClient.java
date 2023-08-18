package org.apache.accumulo.core.rpc.clients;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.tasks.thrift.TaskManager.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskManagerThriftClient extends ThriftClientTypes<Client> implements ManagerClient<Client> {

  private static Logger LOG = LoggerFactory.getLogger(TaskManagerThriftClient.class);

  public TaskManagerThriftClient(String serviceName) {
    super(serviceName, new Client.Factory());
  }

  @Override
  public Client getConnection(ClientContext context) {
    return getManagerConnection(LOG, this, context);
  }

}
