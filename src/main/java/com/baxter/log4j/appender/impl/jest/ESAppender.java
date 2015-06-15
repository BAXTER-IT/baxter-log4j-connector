package com.baxter.log4j.appender.impl.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import com.baxter.log4j.appender.impl.ESAppenderBase;

public class ESAppender extends ESAppenderBase
{
  private JestClient client;

  @Override
  protected void processEvents(List<LoggingEvent> currentEvents) throws Exception
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

  protected void writeBasic(Map<String, Object> json, LoggingEvent loggingEvent)
  {
	json.put("hostName", getElasticSearchHost());
	json.put("rawtimestamp", loggingEvent.getTimeStamp());
	json.put("timestamp", dateFormat.format(loggingEvent.getTimeStamp()));
	json.put("logger", loggingEvent.getLoggerName());
	json.put("level", loggingEvent.getLevel().toString());
	json.put("message", getLayout().format(loggingEvent));
	json.put("clientHost", clientHost);
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

  @Override
  public void close()
  {
	client.shutdownClient();
	super.close();
  }

  @Override
  public void activateOptions()
  {
	final JestClientFactory factory = new JestClientFactory();
	factory.setClientConfig(new ClientConfig.Builder("http://" + elasticSearchHost + ":" + port).multiThreaded(true).build());

	client = factory.getObject();

	super.activateOptions();
  }
}
