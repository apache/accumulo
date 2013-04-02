package org.apache.accumulo.server.master.state;

import org.apache.accumulo.server.master.state.DistributedStoreException;

public class IllegalDSException extends DistributedStoreException {

  public IllegalDSException(String why) {
    super(why);
  }

  private static final long serialVersionUID = 1L;
  
}
