package org.apache.accumulo.core.util.format;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;


public class FormatterFactory
{
    private static final Logger log = Logger.getLogger(FormatterFactory.class);
    
    public static Formatter getFormatter(Class<? extends Formatter> formatterClass, Iterable<Entry<Key, Value>> scanner, boolean printTimestamps)
    {
        Formatter formatter = null;
        try {
            formatter = formatterClass.newInstance();
        } catch (Exception e) {
            log.warn("Unable to instantiate formatter. Using default formatter.", e);
            formatter = new DefaultFormatter();
        }
        formatter.initialize(scanner, printTimestamps);
        return formatter;
    }
    
    public static Formatter getDefaultFormatter(Iterable<Entry<Key, Value>> scanner, boolean printTimestamps)
    {
        return getFormatter(DefaultFormatter.class, scanner, printTimestamps);
    }
}
