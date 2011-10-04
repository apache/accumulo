/*
//
// regtime.h
//
// Copyright (C) 1996 Limit Point Systems, Inc.
//
// Author: Curtis Janssen <cljanss@limitpt.com>
// Maintainer: LPS
//
// This file is part of the SC Toolkit.
//
// The SC Toolkit is free software; you can redistribute it and/or modify
// it under the terms of the GNU Library General Public License as published by
// the Free Software Foundation; either version 2, or (at your option)
// any later version.
//
// The SC Toolkit is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Library General Public License for more details.
//
// You should have received a copy of the GNU Library General Public License
// along with the SC Toolkit; see the file COPYING.LIB.  If not, write to
// the Free Software Foundation, 675 Mass Ave, Cambridge, MA 02139, USA.
//
// The U.S. Government is granted a limited license as per AL 91-7.
//
*/

package util;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Vector;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
* <code>RegionTimer</code> is a class used to gather detailed timing
* information which is useful in finding performance bottlenecks. Usually, only
* one RegionTimer is needed, but it is not reentrant, so if you plan to use
* threads, you should have one <code>RegionTimer</code> per thread.
*
* <p>
* Use of the <code>RegionTimer</code> is straightforward. It is first
* instantiated with a <code>String</code> parameter which is the name of the
* region to be timed (usually the name of the program being run). Subregions to
* be timed are delimitted by calls to the <code>enter</code> and
* <code>exit</code> or <code>change</code>. Subregions can be nested, but
* you cannot reenter a subregion.
*
* <p>
* <strong>Example: </strong> <blockquote>
*
* <pre>
* RegionTimer t = new RegionTimer(&quot;test&quot;);
*
* t.enter(&quot;i loop&quot;);
* for (int i = 0; i &lt; 1000; i++)
* {
*     t.enter(&quot;j loop&quot;);
*     for (int j = 0; j &lt; 100; j++)
*     {
*         t.enter(&quot;foo&quot;);
*         double foo = 3.14 / (double) j;
*         t.change(&quot;bar&quot;);
*         double bar = foo * (double) j;
*         t.exit(&quot;bar&quot;);
*     }
*     t.exit(&quot;j loop&quot;);
* }
* t.exit(&quot;i loop&quot;);
* System.out.println(t);
* </pre>
*
* </blockquote>
*
* <p>
* <strong>Produces the following output: </strong> <blockquote>
*
* <pre>
*
*
*                             Wall Time
*                test:        2.41
*                  i loop:    2.39
*                    j loop:  2.38
*                      bar:   0.48
*                      foo:   0.49
*
*
* </pre>
*
* </blockquote>
*/

public class RegionTimer
{

	public static ThreadLocal<RegionTimer> threadTimer = null;

	private TimedRegion top_;

	private TimedRegion current_;

	protected boolean mEnabled = true;

	protected String mTopName;

	private Log logger = LogFactory.getLog(RegionTimer.class);

	/**
	 * Use this method to enable or disable collection of timing statistics. All
	 * timing information will be lost.
	 *
	 * @param shouldEnable
	 *            false to disable statistics collection
	 */
	public void enable(boolean shouldEnable)
	{
		mEnabled = shouldEnable;
		reset();
	}

	/**
	 * The one and only constructor.
	 *
	 * @param topname
	 *            The name of the top-most region (usually the program name).
	 */
	public RegionTimer(String topname)
	{
		mTopName = topname;
		reset();
	}

	/**
	 * Restarts the region timer. All previous timing information is lost.
	 */
	public void reset()
	{
		if (mEnabled)
		{
			top_ = new TimedRegion(mTopName);
			top_.wall_enter(get_wall_time());
			current_ = top_;
		} else
		{
			top_ = null;
			current_ = null;
		}
	}

	/**
	 * Enter a subregion.
	 *
	 * @param name
	 *            Name of the region being entered. Must be unique.
	 */
	public void enter(String name)
	{
		if (!mEnabled)
		{
			return;
		}
		current_ = current_.findinsubregion(name);
		current_.wall_enter(get_wall_time());
	}

	/**
	 * Set the count on the current region.
	 *
	 * @param count
	 *            the new count value
	 */
	public void setCount(int count)
	{
		if (!mEnabled || current_ == null)
		{
			return;
		}
		current_.count_ = count;
	}

	/**
	 * Leave a subregion.
	 *
	 * @param name
	 *            Name of the region being exited.
	 */
	public void exit(String name)
	{
		if (!mEnabled)
		{
			return;
		}

		double wall = get_wall_time();

		if (current_ == null || (name != null && !name.equals(current_.name())))
		{
			logger.info("TimeRegion::exit(\"" + name + "\"):"
					+ " current region" + " (\"" + current_.name() + "\")"
					+ " doesn't match name");
			return;
		}

		current_.wall_exit(wall);
		if (current_.up() == null)
		{
			logger.info("RegionTimer::exit: already at top level");
			return;
		}
		current_ = current_.up();
	}

	/**
	 * Change the subregion. <code>change("name 2")</code> is equivalent to
	 * <code>exit("name 1"); enter("name 2")</code>.
	 *
	 * @param newname
	 *            Name of the region being entered. Must be unique.
	 */
	public void change(String newname)
	{
		if (!mEnabled)
		{
			return;
		}

		double wall = get_wall_time();

		if (current_ == null)
		{
			logger.info("RegionTimer::change: current region is null");
			return;
		}

		current_.wall_exit(wall);

		if (current_.up() == null)
		{
			logger.info("RegionTimer::change: already at top level");
			return;
		}

		current_ = current_.up();
		current_ = current_.findinsubregion(newname);
		current_.wall_enter(get_wall_time());
	}

	public void subtime(String newname, double timeInSeconds)
	{
		if (!mEnabled)
		{
			return;
		}

		current_ = current_.findinsubregion(newname);
		current_.wall_enter(0.0);
		current_.wall_exit(timeInSeconds);
		current_ = current_.up();
	}

	/**
	 * Returns a string containing the current timing information.
	 *
	 * @return string containing the current timing information
	 */
	public String toString()
	{
		if (!mEnabled)
		{
			return "Reporting is disabled\n";
		}

		update_top();

		int n = nregion();
		Vector<Double> wall_time = new Vector<Double>(n);
		get_wall_times(wall_time);

		Vector<Integer> count = new Vector<Integer>(n);
		get_counts(count);

		Vector<String> names = new Vector<String>(n);
		get_region_names(names);

		Vector<Integer> depth = new Vector<Integer>(n);
		get_depth(depth);

		int i, j;
		int maxwidth = 0, maxcount = 0;
		double maxwalltime = 0.0;
		for (i = 0; i < n; i++)
		{
			String name_i = (String) names.elementAt(i);
			int depth_i = ((Integer) depth.elementAt(i)).intValue();
			int count_i = ((Integer) count.elementAt(i)).intValue();
			double wall_time_i = ((Double) wall_time.elementAt(i))
					.doubleValue();
			int width = name_i.length() + 2 * depth_i + 2;
			if (width > maxwidth)
				maxwidth = width;
			if (wall_time_i > maxwalltime)
				maxwalltime = wall_time_i;
			if (count_i > maxcount)
				maxcount = count_i;
		}

		String pattern = "0.00";
		int maxwallwidth = 4;
		while (maxwalltime >= 10.0)
		{
			maxwalltime /= 10.0;
			pattern = "#" + pattern;
			maxwallwidth++;
		}

		StringBuffer result = new StringBuffer(512);
		String nl = System.getProperty("line.separator");
		for (i = 0; i < maxwidth; i++)
			result.append(" ");
		result.append(" Wall Time\tCount");
		result.append(nl);

		NumberFormat nf = DecimalFormat.getNumberInstance();

		if (nf instanceof DecimalFormat)
		{
			DecimalFormat df = (DecimalFormat) nf;
			df.applyPattern(pattern);
		}

		int maxcountwidth = Integer.toString(maxcount).length();

		for (i = 0; i < n; i++)
		{
			String name_i = (String) names.elementAt(i);
			int depth_i = ((Integer) depth.elementAt(i)).intValue();
			double wall_time_i = ((Double) wall_time.elementAt(i))
					.doubleValue();
			int count_i = ((Integer) count.elementAt(i)).intValue();
			int width = name_i.length() + 2 * depth_i + 2;
			for (j = 0; j < depth_i; j++)
				result.append("  ");
			result.append(name_i).append(": ");
			for (j = width; j < maxwidth; j++)
				result.append(" ");
			String wti = nf.format(wall_time_i);
			int pad = maxwallwidth - wti.length();
			for (j = 0; j < pad; j++)
				result.append(" ");
			result.append(" ").append(wti).append("\t");
			pad = maxcountwidth - Integer.toString(count_i).length();
			for (j = 0; j < pad; j++)
				result.append(" ");
			result.append(count_i).append(nl);
		}

		return result.toString();
	}

	public double getTime(String soughtname)
	{
		return top_.getRegionTime(soughtname);
	}

	public TimedRegion getRegion(String soughtname)
	{
		return top_.getRegion(soughtname);
	}


	// ///////////////////////////////////////////////////////////////////////
	// public static methods
	private static RegionTimer sRegionTimer = null;

	private static boolean sStaticTimer = false;

	// only allow use of static timers if iowa.globaltimer is true. this
	// does not affect the explicit use of a local instance of a regiontimer.
	static
	{
		String val = System.getProperty("llnl.globaltimer");
		if (val != null)
		{
			val = val.toUpperCase();
			if (val.equals("YES") || val.equals("Y") || val.equals("1")
					|| val.equals("TRUE") || val.equals("T"))
				sStaticTimer = true;
		}
	}

	/**
	 * Determines whether global timing statistics will be gathered.
	 *
	 * @param b
	 *            Set to <code>true</code> if global statistics are to be
	 *            collected.
	 */
	public static void setUseGlobalTimer(boolean b)
	{
		sStaticTimer = b;
	}

	/**
	 * Returns the current global <code>RegionTimer</code>.
	 */
	public static RegionTimer getGlobalRegionTimer()
	{
		return sRegionTimer;
	}

	/**
	 * Sets the global <code>RegionTimer</code>.
	 *
	 * @param r
	 *            A <code>RegionTimer</code> to be made global.
	 */
	public static void setGlobalRegionTimer(RegionTimer r)
	{
		sRegionTimer = r;
	}

	/**
	 * Calls <code>enter</code> on the global <code>RegionTimer</code>.
	 *
	 * @param name
	 *            Name of the region being entered.
	 * @see #enter(String)
	 */
	public static void timeEnter(String name)
	{
		if (!sStaticTimer || sRegionTimer == null)
			return;
		sRegionTimer.enter(name);
	}

	/**
	 * Calls <code>exit</code> on the global <code>RegionTimer</code>.
	 *
	 * @param name
	 *            Name of the region being exited.
	 * @see #exit(String)
	 */
	public static void timeExit(String name)
	{
		if (!sStaticTimer || sRegionTimer == null)
			return;
		sRegionTimer.exit(name);
	}

	/**
	 * Calls <code>change</code> on the global <code>RegionTimer</code>.
	 *
	 * @param name
	 *            Name of the region being entered.
	 * @see #change(String)
	 */
	public static void timeChange(String name)
	{
		if (!sStaticTimer || sRegionTimer == null)
			return;
		sRegionTimer.change(name);
	}

	/**
	 * Calls <code>toString</code> on the global <code>RegionTimer</code>.
	 *
	 * @return string containing timing information
	 * @see #toString()
	 */
	public static String timeToString()
	{
		if (!sStaticTimer || sRegionTimer == null)
			return null;
		return sRegionTimer.toString();
	}

	/**
	 * Dumps current timing info to the specified stream
	 */
	public static void dumpTiming(PrintStream inOut)
	{
		if (!sStaticTimer || sRegionTimer == null)
			return;
		String dump = sRegionTimer.toString();
		if (dump != null)
			inOut.println(dump);
	}

	/**
	 * Dumps current timing info to stdout
	 */
	public static void dumpTiming()
	{
		dumpTiming(System.out);
	}

	private void update_top()
	{
		top_.wall_exit(get_wall_time());
	}

	private int nregion()
	{
		return top_.nregion();
	}

	private void get_region_names(Vector<String> names)
	{
		top_.get_region_names(names);
	}

	private void get_wall_times(Vector<Double> times)
	{
		top_.get_wall_times(times);
	}

	private void get_counts(Vector<Integer> times)
	{
		top_.get_counts(times);
	}

	private void get_depth(Vector<Integer> depth)
	{
		top_.get_depth(depth, 0);
	}

	private double get_wall_time()
	{
		long tnow = System.currentTimeMillis();
		return tnow * 0.001d;
	}

	// ///////////////////////////////////////////////////////////////////////

	public final class TimedRegion
	{

		private String name_;

		private TimedRegion up_;

		private TimedRegion subregions_;

		private TimedRegion next_;

		private TimedRegion prev_;

		private double wall_time_;

		private double wall_enter_;

		private int count_;

		private TimedRegion insert_after(String name)
		{
			TimedRegion res = new TimedRegion(name);
			res.prev_ = this;
			res.next_ = this.next_;
			if (res.next_ != null)
				res.next_.prev_ = res;
			res.up_ = up_;
			this.next_ = res;
			return res;
		}

		/**
		 * Get the region time of the section
		 * @param soughtname the time section
		 * @return the region time
		 */
		public double getRegionTime(String soughtname)
		{
			if (soughtname.equals(name_))
				return wall_time_;
			rewind_subregions();
			if (subregions_ != null)
			{
				for (TimedRegion i = subregions_; i != null; i = i.next_)
				{
					double val = i.getRegionTime(soughtname);
					if (val != Double.NEGATIVE_INFINITY)
						return val;
				}
			}
			return Double.NEGATIVE_INFINITY;
		}

		public TimedRegion getRegion(String soughtname)
		{
			if (soughtname.equals(name_))
				return this;
			rewind_subregions();
			if (subregions_ != null)
			{
				for (TimedRegion i = subregions_; i != null; i = i.next_)
				{
					TimedRegion val = i.getRegion(soughtname);
					if (val != null)
						return val;
				}
			}
			return null;
		}

		public double getTime()
		{
			return this.wall_time_;
		}

		public int getCount()
		{
			return this.count_;
		}

		private TimedRegion insert_before(String name)
		{
			TimedRegion res = new TimedRegion(name);
			res.next_ = this;
			res.prev_ = this.prev_;
			if (res.prev_ != null)
				res.prev_.next_ = res;
			res.up_ = up_;
			this.prev_ = res;
			return res;
		}

		private void rewind_subregions()
		{
			if (subregions_ != null)
				while (subregions_.prev_ != null)
					subregions_ = subregions_.prev_;
		}

		public TimedRegion(String name)
		{
			name_ = name;
			wall_time_ = 0.0;
			up_ = null;
			subregions_ = null;
			next_ = prev_ = null;
			count_ = 0;
		}

		public String name()
		{
			return name_;
		}

		public int nregion()
		{
			int n = 1;
			rewind_subregions();
			for (TimedRegion i = subregions_; i != null; i = i.next_)
				n += i.nregion();
			return n;
		}

		public TimedRegion findinsubregion(String soughtname)
		{
			if (subregions_ == null)
			{
				subregions_ = new TimedRegion(soughtname);
				subregions_.up_ = this;
				return subregions_;
			}

			int cmp = subregions_.name_.compareTo(soughtname);
			if (cmp < 0)
			{
				do
				{
					if (subregions_.next_ == null)
					{
						return subregions_.insert_after(soughtname);
					}
					subregions_ = subregions_.next_;
				} while ((cmp = subregions_.name_.compareTo(soughtname)) < 0);
				if (cmp == 0)
					return subregions_;
				subregions_ = subregions_.insert_before(soughtname);
			} else if (cmp > 0)
			{
				do
				{
					if (subregions_.prev_ == null)
					{
						return subregions_.insert_before(soughtname);
					}
					subregions_ = subregions_.prev_;
				} while ((cmp = subregions_.name_.compareTo(soughtname)) > 0);
				if (cmp == 0)
					return subregions_;
				subregions_ = subregions_.insert_after(soughtname);
			}
			return subregions_;
		}

		public void wall_enter(double t)
		{
			count_++;
			wall_enter_ = t;
		}

		public void wall_exit(double t)
		{
			wall_time_ += t - wall_enter_;
			wall_enter_ = t;
		}

		public TimedRegion up()
		{
			return up_;
		}

		public void get_region_names(Vector<String> names)
		{
			names.addElement(name());
			rewind_subregions();
			for (TimedRegion i = subregions_; i != null; i = i.next_)
				i.get_region_names(names);
		}

		public void get_wall_times(Vector<Double> t)
		{
			t.addElement(new Double(wall_time_));
			rewind_subregions();
			for (TimedRegion i = subregions_; i != null; i = i.next_)
				i.get_wall_times(t);
		}

		public void get_counts(Vector<Integer> t)
		{
			t.addElement(new Integer(count_));
			rewind_subregions();
			for (TimedRegion i = subregions_; i != null; i = i.next_)
				i.get_counts(t);
		}

		public void get_depth(Vector<Integer> depth, int depthl)
		{
			depth.addElement(new Integer(depthl));
			rewind_subregions();
			for (TimedRegion i = subregions_; i != null; i = i.next_)
				i.get_depth(depth, depthl + 1);
		}
	}
}
