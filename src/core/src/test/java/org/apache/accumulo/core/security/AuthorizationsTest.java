package org.apache.accumulo.core.security;

import static org.junit.Assert.*;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ByteArraySet;
import org.junit.Test;


public class AuthorizationsTest {
    
        
    @Test
    public void testSetOfByteArrays() {
        assertTrue(ByteArraySet.fromStrings("a", "b","c").contains("a".getBytes()));
    }

    @Test
    public void testEncodeDecode() {
        Authorizations a = new Authorizations("a", "abcdefg", "hijklmno");
        byte[] array = a.getAuthorizationsArray();
        Authorizations b = new Authorizations(array);
        assertEquals(a, b);
    }
    
}
