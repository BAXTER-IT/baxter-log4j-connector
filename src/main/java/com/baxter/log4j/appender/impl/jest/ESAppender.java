package com.baxter.log4j.appender.impl.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

public class ESAppender extends AppenderSkeleton
{
  private final ReentrantLock lock = new ReentrantLock();
  List<LoggingEvent> events = new LinkedList<LoggingEvent>();
  final Semaphore semaphore = new Semaphore(1);

  private JestClient client;

  WorkerThread thread = new WorkerThread();

  private String host;
  private int port;
  private String clusterName;
  private String index, type;
  private int queuingWarningLevel;
  private PatternLayout layout;

  public ESAppender()
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

	private void processEvents(List<LoggingEvent> currentEvents) throws Exception
	{
	  final Builder bulk = new Bulk.Builder();

	  for (final LoggingEvent loggingEvent : currentEvents)
	  {
		final Map<String, Object> data = new HashMap<String, Object>();

		writeBasic(data, loggingEvent);
		writeThrowable(data, loggingEvent);

		bulk.addAction(new Index.Builder(data).index(index).type(type).build());
	  }

	  if (currentEvents.size() > queuingWarningLevel)
	  {
		final long startTime = System.currentTimeMillis();
		client.execute(bulk.build());
		System.err.printf("currentEvents.size(): %1$d queuingWarningLevel: %2$d processTime: %3$d\n", currentEvents.size(),
		    queuingWarningLevel, (System.currentTimeMillis() - startTime));
	  }
	  else
	  {
		client.execute(bulk.build());
	  }
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
	  json.put("timestamp", event.getTimeStamp());
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
  }

  @Override
  public void close()
  {
	client.shutdownClient();
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

	final JestClientFactory factory = new JestClientFactory();
	factory.setClientConfig(new ClientConfig.Builder("http://" + host + ":" + port).multiThreaded(true).build());
	client = factory.getObject();

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

  public int getQueuingWarningLevel()
  {
	return queuingWarningLevel;
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
}
