package org.apache.accumulo.core.util;

import java.util.Collection;

public final class StringUtil {
    public static String join(Collection<String> strings, String sep)
    {
        int last = 0;
        StringBuilder ret = new StringBuilder();
        for (String s : strings)
        {
            ret.append(s);
            last = ret.length();
            ret.append(sep);
        }
        ret.delete(last, ret.length());
        return ret.toString();
    }
}
