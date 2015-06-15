package com.baxter.log4j.appender.impl.transportclient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.List;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.baxter.log4j.appender.impl.ESAppenderBase;

public class ESAppender extends ESAppenderBase
{
  private TransportClient client;

  public ESAppender()
  {
	super();
  }

  @Override
  protected void processEvents(List<LoggingEvent> currentEvents) throws Exception
  {
	final BulkRequestBuilder bulkRequest = client.prepareBulk();

	for (final LoggingEvent loggingEvent : currentEvents)
	{
	  final XContentBuilder source = jsonBuilder().startObject();
	  source.field("rawtimestamp", loggingEvent.getTimeStamp());
	  source.field("timestamp", dateFormat.format(loggingEvent.getTimeStamp()));
	  source.field("message", loggingEvent.getMessage());
	  source.field("logger", loggingEvent.getLoggerName());
	  source.field("level", loggingEvent.getLevel().toString());
	  source.field("clientHost", clientHost);

	  final ThrowableInformation throwableInformation = loggingEvent.getThrowableInformation();
	  if (throwableInformation != null)
	  {
		final Throwable throwable = throwableInformation.getThrowable();

		source.field("className", throwable.getClass().getCanonicalName());
		source.field("stackTrace", getStackTrace(throwable));
	  }

	  bulkRequest.add(client.prepareIndex(index, type).setSource(source.endObject()));
	}

	if (currentEvents.size() > queuingWarningLevel)
	{
	  final long startTime = System.currentTimeMillis();
	  final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
	  System.err.printf("currentEvents.size(): %1$d queuingWarningLevel: %2$d processTime: %3$d\n", currentEvents.size(),
		  queuingWarningLevel, (System.currentTimeMillis() - startTime));
	}
	else
	{
	  final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
	}
  }

  @Override
  public void close()
  {
	client.close();

	super.close();
  }

  @Override
  public void activateOptions()
  {
	super.activateOptions();

	client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(elasticSearchHost, port));
  }

}
