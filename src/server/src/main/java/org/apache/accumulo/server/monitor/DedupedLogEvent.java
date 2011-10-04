package org.apache.accumulo.server.monitor;

import org.apache.log4j.spi.LoggingEvent;

public class DedupedLogEvent {
	
	private LoggingEvent event;
	private int count = 0;
	private int hash = -1;
	
	public DedupedLogEvent(LoggingEvent event) {
		this(event, 1);
	}
	
	public DedupedLogEvent(LoggingEvent event, int count) {
		this.event = event;
		this.count = count;
	}
	
	public LoggingEvent getEvent() {
		return event;
	}
	
	public int getCount() {
		return count;
	}
	
	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public int hashCode()
	{
		if (hash == -1) {
			String eventId = event.getMDC("application").toString()+":"+event.getLevel().toString()+":"+event.getMessage().toString();
			hash = eventId.hashCode();
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object obj) {
	    if (obj instanceof DedupedLogEvent)
	        return this.event.equals(event);
	    return false;
	}

	@Override
	public String toString()
	{
		return event.getMDC("application").toString()+":"+event.getLevel().toString()+":"+event.getMessage().toString();
	}
}
