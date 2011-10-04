package org.apache.accumulo.core.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.util.StringUtil;
import org.junit.Test;

public class TestStringUtil {

    static List<String> parts(Object ... parts) {
        List<String> result = new ArrayList<String>();
        for (Object obj : parts) {
            result.add(obj.toString());
        }
        return result;
    }
    
    @Test
    public void testJoin() {
        assertEquals(StringUtil.join(parts(), ","), "");
        assertEquals(StringUtil.join(parts("a","b","c"), ","), "a,b,c");
        assertEquals(StringUtil.join(parts("a"), ","), "a");
        assertEquals(StringUtil.join(parts("a","a"), ","), "a,a");
    }

}
