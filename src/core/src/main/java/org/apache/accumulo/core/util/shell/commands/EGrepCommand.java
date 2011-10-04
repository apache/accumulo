package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.RegExIterator;


public class EGrepCommand extends GrepCommand {
	@Override
	protected void setUpIterator(int prio, String name, String term, BatchScanner scanner) throws IOException {
		if (prio < 0)
			throw new IllegalArgumentException("Priority < 0 " + prio);

		IteratorSetting si = new IteratorSetting(prio, name, RegExIterator.class);
		RegExIterator.setRegexs(si, term, term, term, term, true);
		scanner.addScanIterator(si);
	}

	@Override
	public String description() {
		return "searches each row, column family, column qualifier and value, in parallel, on the server side (using a java Matcher, so put .* before and after your term if you're not matching the whole element)";
	}

	@Override
	public String usage() {
		return getName() + " <regex>{ <regex>}";
	}
}