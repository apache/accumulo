package org.apache.accumulo.core.trace;

import org.apache.accumulo.core.client.Instance;

public class InstanceUserPassword {
    public Instance instance;
    public String   username;
    public byte[]   password;

    public InstanceUserPassword(Instance instance, String username, String password) {
        this.instance = instance;
        this.username = username;
        this.password = password.getBytes();
    }
}