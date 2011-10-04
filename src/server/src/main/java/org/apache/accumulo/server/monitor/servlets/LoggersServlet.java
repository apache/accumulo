package org.apache.accumulo.server.monitor.servlets;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.master.thrift.LoggerStatus;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.monitor.util.Table;
import org.apache.accumulo.server.monitor.util.celltypes.LoggerLinkType;


public class LoggersServlet extends BasicServlet {

	private static final long serialVersionUID = 1L;
	private static final LoggerStatus NO_STATUS = new LoggerStatus();

	@Override
	protected String getTitle(HttpServletRequest req) {
		return "Logger Server Status";
	}

	@Override
	protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws Exception {
		String loggerAddress = req.getParameter("s");
		log.debug("loggerAddr = " + loggerAddress);

		if (loggerAddress == null || loggerAddress.isEmpty()) {

			ArrayList<LoggerStatus> loggers = new ArrayList<LoggerStatus>();
			if (Monitor.getMmi() != null)
				loggers.addAll(Monitor.getMmi().loggers);

			Table loggerList = new Table("loggers", "Logger&nbsp;Servers");

			doLoggerServerList(req, sb, loggers, loggerList);
			return;
		}
	}

	static void doLoggerServerList(HttpServletRequest req, StringBuilder sb, List<LoggerStatus> loggers, Table loggerList) {
		loggerList.addSortableColumn("Server", new LoggerLinkType(), null);

		for (LoggerStatus status : loggers) {
			if (status == null)
				status = NO_STATUS;
			RecoveryStatus s = new RecoveryStatus();
			s.host = status.logger;
			loggerList.addRow(s);
		}
		loggerList.generate(req, sb);
	}

}
