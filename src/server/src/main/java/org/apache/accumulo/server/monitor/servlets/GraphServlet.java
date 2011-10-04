package org.apache.accumulo.server.monitor.servlets;

import java.awt.Color;
import java.awt.geom.Ellipse2D;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;


public class GraphServlet extends HttpServlet {

    public static final int GRAPH_HEIGHT = 150;

    public static final int GRAPH_WIDTH = 400;

    private static final long serialVersionUID = 1L;
    
    static private final Logger log = Logger.getLogger(GraphServlet.class); 
    
    static Double percent(Double d) {
        if (d == null)
            return d;
        return new Double(d.doubleValue() * 100);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        ServletOutputStream out = resp.getOutputStream();
       
        String which = req.getParameter("name");
        String title = "Unknown";

        TimeSeries dataset = new TimeSeries("Values", FixedMillisecond.class);
        
        try {
        	boolean showShapes = false;
        	
            if (which.equals("load")) {
                title = "Load Average";
                for (Pair<Long, Double> d : Monitor.getLoadOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("recovery")) {
                title = "Recoveries";
                for (Pair<Long, Integer> d : Monitor.getRecoveriesOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("ingest_entries")) {
                title = "Ingest (Entries/s)";
                for (Pair<Long, Double> d : Monitor.getIngestRateOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("ingest_bytes")) {
                title = "Ingest (MB/s)";
                for (Pair<Long, Double> d : Monitor.getIngestByteRateOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("minc")) {
            	title = "Minor Compactions";
                for (Pair<Long, Integer> d : Monitor.getMinorCompactionsOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("majc")) {
            	title = "Major Compactions";
                for (Pair<Long, Integer> d : Monitor.getMajorCompactionsOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("lookups")) {
            	title = "Scan Sessions";
                for (Pair<Long, Double> d : Monitor.getLookupsOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("scan_entries")) {
            	title = "Scan (Entries/s)";
                for (Pair<Long, Integer> d : Monitor.getQueryRateOverTime()) {
                    dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
                }
            } else if (which.equals("scan_bytes")) {
            	title = "Scan (MB/s)";
            	for (Pair<Long, Double> d : Monitor.getQueryByteRateOverTime()) {
            		dataset.add(new FixedMillisecond(d.getFirst()), d.getSecond());
            	}
            } else if (which.equals("index_cache")) {
            	title = "Index Cache Hit Rate";
            	for (Pair<Long, Double> d : Monitor.getIndexCacheHitRateOverTime()) {
            		dataset.add(new FixedMillisecond(d.getFirst()), percent(d.getSecond()));
            	}
            	showShapes = true;
            } else if (which.equals("data_cache")) {
            	title = "Data Cache Hit Rate";
            	for (Pair<Long, Double> d : Monitor.getDataCacheHitRateOverTime()) {
            		dataset.add(new FixedMillisecond(d.getFirst()), percent(d.getSecond()));
            	}
            	showShapes = true;
            } else {
                throw new UnavailableException("Unknown graph name");
            }
            TimeSeriesCollection collection = new TimeSeriesCollection();
            collection.addSeries(dataset);
            JFreeChart chart = ChartFactory.createTimeSeriesChart(title, "", "", collection, false, false, false);
            
            if(showShapes){
            	XYLineAndShapeRenderer rend = new XYLineAndShapeRenderer();
            	rend.setBaseShape(new Ellipse2D.Double(0,0,1,1));
            	rend.setSeriesShape(0,new Ellipse2D.Double(0,0,1,1));
            	chart.getXYPlot().setRenderer(rend);
            }
            
            chart.setBackgroundPaint(Color.white);
            chart.setBorderVisible(true);
            resp.setContentType("image/png");
            ChartUtilities.writeChartAsPNG(out, chart, GRAPH_WIDTH, GRAPH_HEIGHT);
        } catch (Exception ex) {
            log.error(ex, ex);
        } finally {
            out.close();
        }
    }

    
    
 
}
