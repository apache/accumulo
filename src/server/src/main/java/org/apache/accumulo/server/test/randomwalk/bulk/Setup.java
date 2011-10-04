package org.apache.accumulo.server.test.randomwalk.bulk;


import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;


@SuppressWarnings("deprecation")
public class Setup extends Test  {
    
    private static final int CORE_POOL_SIZE = 8;
    private static final int MAX_POOL_SIZE = CORE_POOL_SIZE;
    static String tableName = null;
    
    @Override
    public void visit(State state, Properties props) throws Exception {
        Random rand = new Random();
        tableName = Integer.toHexString(Math.abs(rand.nextInt()));
        log.info("Starting bulk test on " + tableName);
        
        List<PerColumnIteratorConfig> aggregators = 
            Collections.singletonList(
                    new PerColumnIteratorConfig(
                            new Text("cf".getBytes()),
                            null,
                            org.apache.accumulo.core.iterators.aggregation.StringSummation.class.getName()));
        try {
            if (!state.getConnector().tableOperations().exists(getTableName()))
            {
            	state.getConnector().tableOperations().create(getTableName());
            	state.getConnector().tableOperations().addAggregators(getTableName(), aggregators);
            }
        } catch (TableExistsException ex) {
            // expected if there are multiple walkers
        }
        state.set("rand", rand);
        state.set("fs", FileSystem.get(CachedConfiguration.getInstance()));

        BlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
        ThreadFactory factory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Daemon(new LoggingRunnable(log, r));
            }
        };
        ThreadPoolExecutor e = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, 1, TimeUnit.SECONDS, q, factory); 
        
        state.set("pool", e);
    }

    public static String getTableName() {
        return tableName;
    }
    
    public static ThreadPoolExecutor  getThreadPool(State state) {
        return (ThreadPoolExecutor) state.get("pool");
    }
    
    public static void run(State state, Runnable r) {
        getThreadPool(state).submit(r);
    }

}
