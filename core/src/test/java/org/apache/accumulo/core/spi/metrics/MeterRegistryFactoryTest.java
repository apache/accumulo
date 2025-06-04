package org.apache.accumulo.core.spi.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.junit.jupiter.api.Test;

import java.util.regex.PatternSyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MeterRegistryFactoryTest {

    /**
     * Verify that when a Null pattern is supplied to {@link MeterRegistryFactory#getMeterFilter(String)}, that we
     * catch a NullPointerException and receive the errorMessage: "patternList must not be null"
     */
    @Test
    public void testNullPatternList () {
        Throwable exception = assertThrows(NullPointerException.class,
                () -> MeterRegistryFactory.getMeterFilter(null), "Expected an NPE");
        assertEquals("patternList must not be null", exception.getMessage());
    }

    /**
     * Verify that when an empty pattern is supplied to {@link MeterRegistryFactory#getMeterFilter(String)}, that we
     * catch an IllegalArgumentException and receive the errorMessage: "patternList must not be empty"
     */
    @Test
    public void testIfEmptyPattern() {
        Throwable exception = assertThrows(IllegalArgumentException.class,
                () -> MeterRegistryFactory.getMeterFilter(""), "Expected an IllegalArgumentException");
        assertEquals("patternList must not be empty", exception.getMessage());

    }

    /**
     * Verify that when an invalid regex pattern is supplied to {@link MeterRegistryFactory#getMeterFilter(String)},
     * that a PatternSyntaxException is thrown.
     */
    @Test
    public void testIfPatternListInvalid() {
        assertThrows(PatternSyntaxException.class,
                () -> MeterRegistryFactory.getMeterFilter("[\\]"), "Expected an PatternSyntaxException");

    }

    /**
     * Verify that when only one pattern is supplied to {@link MeterRegistryFactory#getMeterFilter(String)} and the
     * resulting filter is given an id whose name matches that pattern, that we get a reply of
     * {@link MeterFilterReply#DENY}.
     */
    @Test
    public void testIfSinglePatternMatchesId() {
        MeterFilter filter = MeterRegistryFactory.getMeterFilter("aaa.*");
        Meter.Id id = new Meter.Id("aaaName", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
        MeterFilterReply reply = filter.accept(id);
        assertEquals(MeterFilterReply.DENY, reply, "Expected a reply of DENY");
    }

    /**
     * Verify that when only one pattern is supplied to {@link MeterRegistryFactory#getMeterFilter(String)}, and the
     * resulting filter is given an id whose name does not match that patten, that we get a reply of
     * {@link MeterFilterReply#NEUTRAL}.
     */
    @Test
    public void testIfSinglePatternDoesNotMatch(){
        MeterFilter filter = MeterRegistryFactory.getMeterFilter(("aaa.*"));
        Meter.Id id = new Meter.Id("Wrong", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
        MeterFilterReply reply = filter.accept(id);
        assertEquals(MeterFilterReply.NEUTRAL, reply, "Expected a reply of NEUTRAL");
    }

    /**
     * Verify that when multiple patterns are supplied to {@link MeterRegistryFactory#getMeterFilter(String)}, and the
     * resulting filter is given an id whose name matches at least one of the patterns, that we get a reply of
     * {@link MeterFilterReply#DENY}.
     */
    @Test
    public void testIfMultiplePatternsMatches() {
        MeterFilter filter = MeterRegistryFactory.getMeterFilter(("aaa.*,bbb.*,ccc.*"));
        Meter.Id id = new Meter.Id("aaaRight", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
        MeterFilterReply reply = filter.accept(id);
        assertEquals(MeterFilterReply.DENY, reply, "Expected a reply of DENY");
    }

    /**
     * Verify that when multiple patterns are supplied to {@link MeterRegistryFactory#getMeterFilter(String)}, and the
     * resulting filter is given an id whose name matches none of the patterns, that we get a reply of
     * {@link MeterFilterReply#NEUTRAL}.
     */
    @Test
    public void testMultiplePatternsWithNoMatch() {
        MeterFilter filter = MeterRegistryFactory.getMeterFilter(("aaa.*,bbb.*,ccc.*"));
        Meter.Id id = new Meter.Id("Wrong", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
        MeterFilterReply reply = filter.accept(id);
        assertEquals(MeterFilterReply.NEUTRAL, reply, "Expected a reply of NEUTRAL");
    }
}
