package org.apache.accumulo.server.monitor.servlets;

import java.text.DateFormat;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.monitor.util.Table;
import org.apache.accumulo.server.monitor.util.celltypes.DateTimeType;
import org.apache.accumulo.server.monitor.util.celltypes.DurationType;
import org.apache.accumulo.server.monitor.util.celltypes.NumberType;


public class GcStatusServlet extends BasicServlet {

	private static final long serialVersionUID = 1L;

	protected String getTitle(HttpServletRequest req) {
		return "Garbage Collector Status";
	}

	protected void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
		GCStatus status = Monitor.getGcStatus();

		if (status != null) {
			Table gcActivity = new Table("gcActivity", "Collection&nbsp;Activity");
			gcActivity.addSortableColumn("Activity");
			gcActivity.addSortableColumn("Finished", new DateTimeType(DateFormat.MEDIUM, DateFormat.SHORT), null);
			gcActivity.addSortableColumn("Candidates", new NumberType<Long>(), null);
			gcActivity.addSortableColumn("Deleted", new NumberType<Long>(), null);
			gcActivity.addSortableColumn("In&nbsp;Use", new NumberType<Long>(), null);
			gcActivity.addSortableColumn("Errors", new NumberType<Long>(0l, 1l), null);
			gcActivity.addSortableColumn("Duration", new DurationType(), null);

			if (status.last.finished > 0)
				gcActivity.addRow("File&nbsp;Collection,&nbsp;Last&nbsp;Cycle", status.last.finished, status.last.candidates, status.last.deleted, status.last.inUse, status.last.errors, status.last.finished - status.last.started);
			if (status.current.started > 0)
				gcActivity.addRow("File&nbsp;Collection,&nbsp;Running", status.current.finished, status.current.candidates, status.current.deleted, status.current.inUse, status.current.errors, System.currentTimeMillis() - status.current.started);
			if (status.lastLog.finished > 0)
				gcActivity.addRow("WAL&nbsp;Collection,&nbsp;Last&nbsp;Cycle", status.lastLog.finished, status.lastLog.candidates, status.lastLog.deleted, status.lastLog.inUse, status.lastLog.errors, status.lastLog.finished - status.lastLog.started);
			if (status.currentLog.started > 0)
				gcActivity.addRow("WAL&nbsp;Collection,&nbsp;Running", status.currentLog.finished, status.currentLog.candidates, status.currentLog.deleted, status.currentLog.inUse, status.currentLog.errors, System.currentTimeMillis() - status.currentLog.started);
			gcActivity.generate(req, sb);
		} else {
			banner(sb, "error", "Collector is Unavailable");
		}
	}

}
