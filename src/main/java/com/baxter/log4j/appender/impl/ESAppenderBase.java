package com.baxter.log4j.appender.impl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.helpers.ISO8601DateFormat;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

public abstract class ESAppenderBase extends AppenderSkeleton
{
  private final ReentrantLock lock = new ReentrantLock();
  List<LoggingEvent> events = new LinkedList<LoggingEvent>();
  final Semaphore semaphore = new Semaphore(1);

  WorkerThread thread = new WorkerThread();

  protected String host;
  protected int port;
  private String clusterName;
  protected String index;
  protected String type;
  protected int queuingWarningLevel;

  private final DateFormat df = new ISO8601DateFormat();

  public ESAppenderBase()
  {
	setLayout(new PatternLayout("[%d{ISO8601}] - %m%n"));
  }

  class WorkerThread extends Thread
  {
	boolean isRunning = true;

	@Override
	public void run()
	{
	  while (isRunning)
	  {
		try
		{
		  semaphore.acquire();
		  lock.lock();

		  List<LoggingEvent> currentEvents = null;
		  try
		  {
			currentEvents = events;
			events = new LinkedList<LoggingEvent>();
		  }
		  finally
		  {
			lock.unlock();
		  }

		  if (!currentEvents.isEmpty())
		  {
			processEvents(currentEvents);
		  }
		}
		catch (final Exception e)
		{
		  e.printStackTrace();
		}
	  }
	}

	protected String getStackTrace(Throwable aThrowable)
	{
	  final Writer result = new StringWriter();
	  final PrintWriter printWriter = new PrintWriter(result);
	  aThrowable.printStackTrace(printWriter);
	  return result.toString();
	}
  }

  @Override
  public void close()
  {
	events.clear();
	thread.isRunning = false;
	semaphore.release();
  }

  @Override
  public boolean requiresLayout()
  {
	return true;
  }

  @Override
  protected void append(LoggingEvent event)
  {
	lock.lock();
	try
	{
	  events.add(event);

	  semaphore.release();
	}
	finally
	{
	  lock.unlock();
	}
  }

  @Override
  public void activateOptions()
  {
	super.activateOptions();

	final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
		.put("client.transport.ignore_cluster_name", "true").build();
	thread.start();
  }

  public String getHost()
  {
	return host;
  }

  public void setHost(String host)
  {
	this.host = host;
  }

  public int getPort()
  {
	return port;
  }

  public void setPort(int port)
  {
	this.port = port;
  }

  public String getClusterName()
  {
	return clusterName;
  }

  public void setClusterName(String clusterName)
  {
	this.clusterName = clusterName;
  }

  public String getIndex()
  {
	return index;
  }

  public void setIndex(String index)
  {
	this.index = index;
  }

  public String getType()
  {
	return type;
  }

  public void setType(String type)
  {
	this.type = type;
  }

  public void setQueuingWarningLevel(int queuingWarningLevel)
  {
	this.queuingWarningLevel = queuingWarningLevel;
  }

  @Override
  public String toString()
  {
	return "ESAppender [host=" + host + ", port=" + port + ", clusterName=" + clusterName + ", index=" + index + ", type=" + type
		+ ", queuingWarningLevel=" + queuingWarningLevel + "]";
  }

  protected String getStackTrace(Throwable aThrowable)
  {
	final Writer result = new StringWriter();
	final PrintWriter printWriter = new PrintWriter(result);
	aThrowable.printStackTrace(printWriter);
	return result.toString();
  }

  protected void writeBasic(Map<String, Object> json, LoggingEvent event)
  {
	json.put("hostName", getHost());
	json.put("timestamp", df.format(event.getTimeStamp()));
	json.put("logger", event.getLoggerName());
	json.put("level", event.getLevel().toString());
	json.put("message", getLayout().format(event));
  }

  protected void writeThrowable(Map<String, Object> json, LoggingEvent event)
  {
	final ThrowableInformation ti = event.getThrowableInformation();
	if (ti != null)
	{
	  final Throwable t = ti.getThrowable();

	  json.put("className", t.getClass().getCanonicalName());
	  json.put("stackTrace", getStackTrace(t));
	}
  }

  protected abstract void processEvents(List<LoggingEvent> currentEvents) throws Exception;
}