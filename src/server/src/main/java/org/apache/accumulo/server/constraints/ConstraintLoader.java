package org.apache.accumulo.server.constraints;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.log4j.Logger;


public class ConstraintLoader
{
    private static final Logger log = Logger.getLogger(ConstraintLoader.class);

    public static ConstraintChecker load(String table) throws IOException
    {
        try {

            AccumuloConfiguration conf = ServerConfiguration.getTableConfiguration(table);

            ConstraintChecker cc = new ConstraintChecker();

            for (Entry<String, String> entry : conf) {
                if (entry.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())) {
                    String className = entry.getValue();
                    Class<? extends Constraint> clazz = AccumuloClassLoader.loadClass(className, Constraint.class);
                    log.debug("Loaded constraint " + clazz.getName() + " for " + table);
                    cc.addConstraint(clazz.newInstance());
                }
            }

            return cc;
        } catch (ClassNotFoundException e) {
            log.error(e.toString());
            throw new IOException(e);
        } catch (InstantiationException e) {
            log.error(e.toString());
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            log.error(e.toString());
            throw new IOException(e);
        }
    }
}
