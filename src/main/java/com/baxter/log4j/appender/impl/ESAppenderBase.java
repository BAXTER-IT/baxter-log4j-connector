package com.baxter.log4j.appender.impl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

public abstract class ESAppenderBase extends AppenderSkeleton
{
  LinkedBlockingQueue<LoggingEvent> events = new LinkedBlockingQueue<LoggingEvent>();

  WorkerThread thread = new WorkerThread();

  protected String elasticSearchHost;
  protected String clientHost;
  protected int port;
  private String clusterName;
  protected String index;
  protected String type;
  protected int queuingWarningLevel;

  protected final DateFormat dateFormatForQuery = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSSZ");

  protected final DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
  protected final DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.SSSZ");

  public ESAppenderBase()
  {
	setLayout(new PatternLayout("[%d{ISO8601}] - %m%n"));
  }

  class WorkerThread extends Thread
  {
	boolean isRunning = true;

	WorkerThread()
	{
	  setDaemon(true);
	}

	@Override
	public void run()
	{
	  while (isRunning)
	  {
		try
		{
		  final List<LoggingEvent> currentEvents = new LinkedList<LoggingEvent>();
		  try
		  {
			currentEvents.add(events.take());
			events.drainTo(currentEvents);
		  }
		  finally
		  {
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

	protected String getStackTrace(final Throwable aThrowable)
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
  }

  @Override
  public boolean requiresLayout()
  {
	return true;
  }

  @Override
  protected void append(final LoggingEvent event)
  {
	events.add(event);
  }

  @Override
  public void activateOptions()
  {
	super.activateOptions();

	final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
	    .put("client.transport.ignore_cluster_name", "true").build();
	thread.start();
  }

  public String getElasticSearchHost()
  {
	return elasticSearchHost;
  }

  public void setElasticSearchHost(final String host)
  {
	this.elasticSearchHost = host;
  }

  public int getPort()
  {
	return port;
  }

  public void setPort(final int port)
  {
	this.port = port;
  }

  public String getClusterName()
  {
	return clusterName;
  }

  public void setClusterName(final String clusterName)
  {
	this.clusterName = clusterName;
  }

  public String getIndex()
  {
	return index;
  }

  public void setIndex(final String index)
  {
	this.index = index;
  }

  public String getType()
  {
	return type;
  }

  public void setType(final String type)
  {
	this.type = type;
  }

  public void setQueuingWarningLevel(final int queuingWarningLevel)
  {
	this.queuingWarningLevel = queuingWarningLevel;
  }

  @Override
  public String toString()
  {
	return "ESAppender [host=" + elasticSearchHost + ", port=" + port + ", clusterName=" + clusterName + ", index=" + index
	    + ", type=" + type + ", queuingWarningLevel=" + queuingWarningLevel + ", clientHost=" + clientHost + "]";
  }

  protected String getStackTrace(final Throwable aThrowable)
  {
	final Writer result = new StringWriter();
	final PrintWriter printWriter = new PrintWriter(result);
	aThrowable.printStackTrace(printWriter);
	return result.toString();
  }

  protected abstract void processEvents(List<LoggingEvent> currentEvents) throws Exception;

  public String getClientHost()
  {
	return clientHost;
  }

  public void setClientHost(final String clientHost)
  {
	this.clientHost = clientHost;
  }
}