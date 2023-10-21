package org.apache.accumulo.core.client.admin.compaction;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A file selector for compactions that will select a set of files that is below a
 * configured combined size up to a number of files.  The configured max size is
 * specified using the max.size option (table.compaction.selector.opts.max.size)
 * as a bytes value (e.g. 10M).  The configured max files is specified using the
 * max.files option.
 */
public class CombinedSizeLimitSelector implements CompactionSelector {
    public static final String MAX_SIZE_OPT = "max.size";
    public static final String MAX_FILES_OPT = "max.files";

    // a max combined size or files of 0 essentially disables this selector
    private long maxCombinedSize = 0;
    private int maxFiles = Integer.MAX_VALUE;

    @Override
    public void init(InitParameters iparams) {
        maxCombinedSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(iparams.getOptions().getOrDefault(MAX_SIZE_OPT, "0"));
        maxFiles = Integer.parseInt(iparams.getOptions().getOrDefault(MAX_FILES_OPT, String.valueOf(Integer.MAX_VALUE)));
    }

    @Override
    public Selection select(SelectionParameters sparams) {
        // short circuit if disabled
        if (maxCombinedSize == 0 || maxFiles == 0) {
            return new Selection(Collections.EMPTY_LIST);
        }

        AtomicLong totalSize = new AtomicLong();
        AtomicInteger totalCount = new AtomicInteger();
        // return a selection of files whose combined size is under our limit
        // first filter to those under the max size
        // then sort by size,
        // then filter to those whose combined size is under the max size up to the max number of files
        // return empty selection if only 1 file selected
        return new Selection(sparams.getAvailableFiles().stream().filter(f -> f.getEstimatedSize() <= maxCombinedSize)
                .sorted((a, b) -> Long.signum(a.getEstimatedSize() - b.getEstimatedSize()))
                .filter(f -> totalSize.addAndGet(f.getEstimatedSize()) <= maxCombinedSize && totalCount.incrementAndGet() <= maxFiles)
                .collect(Collectors.collectingAndThen(Collectors.toList(), list -> list.size() <= 1 ? Collections.emptyList() : list)));
    }
}
