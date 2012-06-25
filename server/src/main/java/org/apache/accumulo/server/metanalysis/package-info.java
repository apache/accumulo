/**
 * Provides programs to analyze metadata mutations written to write ahead logs.  
 * 
 * <p>
 * These programs can be used when write ahead logs are archived.   The best way to find
 * which write ahead logs contain metadata mutations is to grep the tablet server logs.  
 * Grep for events where walogs were added to metadata tablets, then take the unique set 
 * of walogs.
 *
 * <p>
 * To use these programs, use IndexMeta to index the metadata mutations in walogs into 
 * Accumulo tables.  Then use FindTable and PrintEvents to analyze those indexes.  
 * FilterMetaiallows filtering walogs down to just metadata events.  This is useful for the
 * case where the walogs need to be exported from the cluster for analysis.
 *
 * @since 1.5
 */
package org.apache.accumulo.server.metanalysis;