package org.apache.accumulo.core.client.admin;

/**
 * The type of ordering to use for the table's entries (default is MILLIS)
 */
public enum TimeType {
	/**
	 * Used to guarantee ordering of data sequentially as inserted
	 */
	LOGICAL,

	/**
	 * This is the default. Tries to ensure that inserted data is stored with
	 * the timestamp closest to the machine's time to the nearest millisecond,
	 * without going backwards to guarantee insertion sequence. Note that using
	 * this time type can cause time to "skip" forward if a machine has a time
	 * that is too far off. NTP is recommended when using this type.
	 */
	MILLIS
}
