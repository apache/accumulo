package org.apache.accumulo.core.client.admin.compaction;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CombinedSizeLimitSelectorTest {

    @Test
    public void testSizeLimit() {
        CombinedSizeLimitSelector selector = new CombinedSizeLimitSelector();
        selector.init(createInitParameters("1G", null));
        CompactionSelector.Selection selection = selector.select(createSelectionParameters("1M", "2M", "500M", "999M", "1G"));
        assertSelected(selection, "1M", "2M", "500M");

        selection = selector.select(createSelectionParameters("1M", "511M", "512M", "999M", "1G"));
        assertSelected(selection, "1M", "511M", "512M");
    }

    @Test
    public void testFilesLimit() {
        CombinedSizeLimitSelector selector = new CombinedSizeLimitSelector();
        selector.init(createInitParameters("1G", "2"));
        CompactionSelector.Selection selection = selector.select(createSelectionParameters("1M", "2M", "500M", "999M", "1G"));
        assertSelected(selection, "1M", "2M");
    }

    @Test
    public void testDoNotCompactOnlyOne() {
        CombinedSizeLimitSelector selector = new CombinedSizeLimitSelector();
        selector.init(createInitParameters("1M", null));
        CompactionSelector.Selection selection = selector.select(createSelectionParameters("1M", "2M", "500M", "999M", "1G"));
        assertSelected(selection);

        selector.init(createInitParameters("1G", "1"));
        selection = selector.select(createSelectionParameters("1M", "2M", "500M", "999M", "1G"));
        assertSelected(selection);
    }

    @Test
    public void testDefaultDisabled() {
        CombinedSizeLimitSelector selector = new CombinedSizeLimitSelector();
        selector.init(createInitParameters(null, null));
        CompactionSelector.Selection selection = selector.select(createSelectionParameters("1M", "2M", "500M", "999M", "1G"));
        assertSelected(selection);
    }

    @Test
    public void testZeroMaxDisabled() {
        CombinedSizeLimitSelector selector = new CombinedSizeLimitSelector();
        selector.init(createInitParameters("0B", null));
        CompactionSelector.Selection selection = selector.select(createSelectionParameters("1M", "2M", "500M", "999M", "1G"));
        assertSelected(selection);
    }

    @Test
    public void testZeroFilesDisabled() {
        CombinedSizeLimitSelector selector = new CombinedSizeLimitSelector();
        selector.init(createInitParameters("1G", "0"));
        CompactionSelector.Selection selection = selector.select(createSelectionParameters("1M", "2M", "500M", "999M", "1G"));
        assertSelected(selection);
    }

    private CompactionSelector.InitParameters createInitParameters(final String maxSize, final String maxFiles) {
        return new CompactionSelector.InitParameters() {
            @Override
            public Map<String, String> getOptions() {
                ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
                if (maxSize != null) {
                    builder.put(CombinedSizeLimitSelector.MAX_SIZE_OPT, maxSize);
                }
                if (maxFiles != null) {
                    builder.put(CombinedSizeLimitSelector.MAX_FILES_OPT, maxFiles);
                }
                return builder.build();
            }

            @Override
            public TableId getTableId() {
                return null;
            }

            @Override
            public PluginEnvironment getEnvironment() {
                return null;
            }
        };
    }

    private CompactionSelector.SelectionParameters createSelectionParameters(final String ... values) {
        return new CompactionSelector.SelectionParameters() {

            @Override
            public PluginEnvironment getEnvironment() {
                return null;
            }

            @Override
            public Collection<CompactableFile> getAvailableFiles() {
                return Collections2.transform(Arrays.asList(values),
                        v -> new CompactableFileImpl(randomURI(), ConfigurationTypeHelper.getFixedMemoryAsBytes(v), 10));
            }

            private URI randomURI() {
                UUID uuid = UUID.randomUUID();
                try {
                    return new URI("file:///accumulo/tables/1/default_tablet/" + uuid);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Collection<Summary> getSummaries(Collection<CompactableFile> files, Predicate<SummarizerConfiguration> summarySelector) {
                return null;
            }

            @Override
            public TableId getTableId() {
                return null;
            }

            @Override
            public Optional<SortedKeyValueIterator<Key, Value>> getSample(CompactableFile cf, SamplerConfiguration sc) {
                return Optional.empty();
            }
        };
    }

    private void assertSelected(CompactionSelector.Selection selection, String ... sizes) {
        assertEquals(Collections2.transform(Arrays.asList(sizes), s -> ConfigurationTypeHelper.getFixedMemoryAsBytes(s)).stream().sorted().collect(Collectors.toList()),
                Collections2.transform(selection.getFilesToCompact(), f -> f.getEstimatedSize()).stream().sorted().collect(Collectors.toList()));
    }
}
