package org.apache.accumulo.test.shell;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;

import java.util.Properties;

public class ErrorMessageCallback {
    private final String errorMessage;

    public ErrorMessageCallback(Properties properties) {
        errorMessage = checkAuths(properties);
    }

    public ErrorMessageCallback() {
        errorMessage = "";
    }

    private String checkAuths(Properties properties) {
        try (AccumuloClient c = Accumulo.newClient().from(properties).build()) {
            return "Current auths for root are: "
                    + c.securityOperations().getUserAuthorizations("root");
        } catch (Exception e) {
            return "Could not check authorizations";
        }
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
