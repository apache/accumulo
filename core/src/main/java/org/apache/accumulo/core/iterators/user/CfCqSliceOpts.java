package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.hadoop.io.Text;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ColumnSliceOpts implements OptionDescriber {
    public static final String OPT_MIN_CF = "minCf";
    public static final String OPT_MIN_CF_DESC = "UTF-8 encoded string representing minimum column family. " +
            "Optional parameter. If minCf and minCq are undefined, the column slice will start at the first column " +
            "of each row. If you want to do an exact match on column families, it's more efficient to leave minCf " +
            "and maxCf undefined and use the scanner's fetchColumnFamily method.";

    public static final String OPT_MIN_CQ = "minCq";
    public static final String OPT_MIN_CQ_DESC = "UTF-8 encoded string representing minimum column qualifier. " +
            "Optional parameter. If minCf and minCq are undefined, the column slice will start at the first column " +
            "of each row.";

    public static final String OPT_MAX_CF = "maxCf";
    public static final String OPT_MAX_CF_DESC = "UTF-8 encoded string representing maximum column family. " +
            "Optional parameter. If minCf and minCq are undefined, the column slice will start at the first column " +
            "of each row. If you want to do an exact match on column families, it's more efficient to leave minCf " +
            "and maxCf undefined and use the scanner's fetchColumnFamily method.";

    public static final String OPT_MAX_CQ = "maxCq";
    public static final String OPT_MAX_CQ_DESC = "UTF-8 encoded string representing maximum column qualifier. " +
            "Optional parameter. If maxCf and MaxCq are undefined, the column slice will end at the last column of " +
            "each row.";

    public static final String OPT_MIN_INCLUSIVE = "minInclusive";
    public static final String OPT_MIN_INCLUSIVE_DESC = "UTF-8 encoded string indicating whether to include the " +
            "minimum column in the slice range. Optional parameter, default is true.";

    public static final String OPT_MAX_INCLUSIVE = "maxInclusive";
    public static final String OPT_MAX_INCLUSIVE_DESC = "UTF-8 encoded string indicating whether to include the " +
            "maximum column in the slice range. Optional parameter, default is true.";

    Text minCf;
    Text minCq;

    Text maxCf;
    Text maxCq;

    boolean minInclusive;
    boolean maxInclusive;

    public ColumnSliceOpts(ColumnSliceOpts o) {
        minCf = new Text(o.minCf);
        minCq = new Text(o.minCq);
        maxCf = new Text(o.maxCf);
        maxCq = new Text(o.maxCq);
        minInclusive = o.minInclusive;
        maxInclusive = o.maxInclusive;
    }

    public ColumnSliceOpts(Map<String,String> options) {
        String optStr = options.get(OPT_MIN_CF);
        minCf = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

        optStr = options.get(OPT_MIN_CQ);
        minCq = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

        optStr = options.get(OPT_MAX_CF);
        maxCf = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

        optStr = options.get(OPT_MAX_CQ);
        maxCq = optStr == null ? new Text() : new Text(optStr.getBytes(UTF_8));

        minInclusive = !options.containsKey(OPT_MIN_INCLUSIVE) ? true : Boolean.valueOf(options.get(OPT_MIN_INCLUSIVE));
        maxInclusive = !options.containsKey(OPT_MIN_INCLUSIVE) ? true : Boolean.valueOf(options.get(OPT_MAX_INCLUSIVE));
    }

    @Override
    public OptionDescriber.IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<String,String>();
        options.put(OPT_MIN_CF, OPT_MIN_CF_DESC);
        options.put(OPT_MIN_CQ, OPT_MIN_CQ_DESC);
        options.put(OPT_MAX_CF, OPT_MAX_CF_DESC);
        options.put(OPT_MAX_CQ, OPT_MAX_CQ_DESC);
        options.put(OPT_MIN_INCLUSIVE, OPT_MIN_INCLUSIVE_DESC);
        options.put(OPT_MAX_INCLUSIVE, OPT_MAX_INCLUSIVE_DESC);
        return new OptionDescriber.IteratorOptions("ColumnSliceFilter",
                "Returns all key/value pairs where the column is between the specified values",
                options, Collections.<String>emptyList());
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        // if you don't specify a max CF and a max CQ, that means there's no upper bounds to the slice. In that case
        // you must not set max inclusive to false.
        boolean upperBoundsExist = (options.get(OPT_MAX_CF) != null && options.get(OPT_MAX_CQ) != null);
        return !upperBoundsExist || !(Boolean.valueOf(options.get(OPT_MAX_INCLUSIVE)));
    }

}
