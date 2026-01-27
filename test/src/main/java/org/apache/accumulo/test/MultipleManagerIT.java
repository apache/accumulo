package org.apache.accumulo.test;

import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.fate.FateManager;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class MultipleManagerIT extends ConfigurableMacBase {
    @Override
    protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
        // TODO add a way to start multiple managers to mini
        super.configure(cfg, hadoopCoreSite);
    }

    @Test
    public void test() throws Exception {

        List<Process> managers = new ArrayList<>();
        for(int i = 0; i<5;i++){
            managers.add(exec(Manager.class));
        }

     var fateMgr =  new FateManager(getServerContext());
     fateMgr.managerWorkers();

     // TODO kill processes
    }
}
