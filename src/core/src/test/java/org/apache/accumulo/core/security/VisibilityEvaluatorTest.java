package org.apache.accumulo.core.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.ByteArraySet;
import org.junit.Test;


public class VisibilityEvaluatorTest
{
    
    @Test
    public void testVisibilityEvaluator()
    throws VisibilityParseException
    {
        VisibilityEvaluator ct = new VisibilityEvaluator(ByteArraySet.fromStrings("one", "two", "three", "four"));
        
        // test for and
        assertTrue("'and' test", ct.evaluate(new ColumnVisibility("one&two")));
        
        // test for or
        assertTrue("'or' test", ct.evaluate(new ColumnVisibility("foor|four")));
        
        // test for and and or
        assertTrue("'and' and 'or' test", ct.evaluate(new ColumnVisibility("(one&two)|(foo&bar)")));
        
        // test for false negatives
        for(String marking:new String[]{"one","one|five","five|one","(one)","(one&two)|(foo&bar)","(one|foo)&three","one|foo|bar","(one|foo)|bar","((one|foo)|bar)&two"})
        {
            assertTrue(marking, ct.evaluate(new ColumnVisibility(marking)));
        }
        
        // test for false positives
        for(String marking:new String[]{"five","one&five","five&one","((one|foo)|bar)&goober"})
        {
            assertFalse(marking, ct.evaluate(new ColumnVisibility(marking)));
        }
        
        // test missing separators; these should throw an exception
        for(String marking:new String[]{"one(five)","(five)one","(one)(two)","a|(b(c))"})
        {
        	try {
        		ct.evaluate(new ColumnVisibility(marking));
        		fail(marking + " failed to throw");
        	} catch (Throwable e) {
        		// all is good
        	}
        }
        
        // test unexpected separator
        for(String marking:new String[]{"&(five)","|(five)","(five)&","five|","a|(b)&","(&five)","(five|)"})
        {
        	try {
        		ct.evaluate(new ColumnVisibility(marking));
        		fail(marking + " failed to throw");
        	} catch (Throwable e) {
        		// all is good
        	}
        }
        
        // test mismatched parentheses
        for(String marking:new String[]{"(",")","(a&b","b|a)"})
        {
        	try {
        		ct.evaluate(new ColumnVisibility(marking));
        		fail(marking + " failed to throw");
        	} catch (Throwable e) {
        		// all is good
        	}
        }
    }
}
