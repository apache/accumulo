package org.apache.accumulo.core.client.mock;

import java.util.Arrays;
import java.util.EnumSet;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;


public class MockUser {
    final EnumSet<SystemPermission> permissions;
    final String name;
    byte[] password;
    Authorizations authorizations;
    
    MockUser(String username, byte[] password, Authorizations auths) {
        this.name = username;
        this.password = Arrays.copyOf(password, password.length);
        this.authorizations = auths;
        this.permissions = EnumSet.noneOf(SystemPermission.class);
    }
}
