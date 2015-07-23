package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


public class ColumnSliceFilter extends Filter implements OptionDescriber {

    private static final Logger log = LoggerFactory.getLogger(ColumnSliceFilter.class);

    private ColumnSliceOpts cso;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        cso = new ColumnSliceOpts(options);
    }

    @Override
    public boolean accept(Key k, Value v) {
        PartialKey inSlice = isKeyInSlice(k);
        return inSlice == PartialKey.ROW_COLFAM_COLQUAL;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        ColumnSliceFilter o = (ColumnSliceFilter)super.deepCopy(env);
        o.cso = new ColumnSliceOpts(cso);
        return o;
    }

    private PartialKey isKeyInSlice(Key k) {
        if (cso.minCf.getLength() > 0) {
            int minCfComp = k.compareColumnFamily(cso.minCf);
            if (minCfComp < 0 || (minCfComp == 0 && !cso.minInclusive)) {
                log.info("cf " + k.getColumnFamily() + " < " + cso.minCf + ", isKeyInSlice == ROW");
                return PartialKey.ROW;
            }
        }
        if (cso.maxCf.getLength() > 0) {
            int maxCfComp = k.compareColumnFamily(cso.maxCf);
            if (maxCfComp > 0 || (maxCfComp == 0 && !cso.maxInclusive)) {
                log.info("cf " + k.getColumnFamily() + " > " + cso.maxCf + ", isKeyInSlice == ROW");
                return PartialKey.ROW;
            }
        }
        // k.colfam is in the "slice".
        if (cso.minCq.getLength() > 0) {
            int minCqComp = k.compareColumnQualifier(cso.minCq);
            if (minCqComp < 0 || (minCqComp == 0 && !cso.minInclusive)) {
                return PartialKey.ROW_COLFAM;
            }
        }
        if (cso.maxCq.getLength() > 0) {
            int maxCqComp = k.compareColumnQualifier(cso.maxCq);
            if (maxCqComp > 0 || (maxCqComp == 0 && !cso.maxInclusive)) {
                return PartialKey.ROW_COLFAM;
            }
        }
        // k.colqual is in the slice.
        return PartialKey.ROW_COLFAM_COLQUAL;
    }

    @Override
    public IteratorOptions describeOptions() {
        return cso.describeOptions();
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        return cso.validateOptions(options);
    }
}
