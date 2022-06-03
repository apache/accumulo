package org.apache.accumulo.server;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

import org.apache.accumulo.core.metadata.schema.Ample;

public class MockAmple {

    public static Ample get() {
        Ample ample = createMock(Ample.class);
        return ample;
    }
}
