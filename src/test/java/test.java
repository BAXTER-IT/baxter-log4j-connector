import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

import com.baxter.log4j.appender.impl.jest.ESAppender;

public class test
{
  public static void main(String[] args) throws IOException, InterruptedException
  {
	final ESAppender es = new ESAppender();

	es.setHost("192.168.6.31");
	es.setPort(9300);
	es.setClusterName("elasticsearch");
	es.setIndex("setindex");
	es.setType("setType");
	es.setLayout(new PatternLayout("[%d{ISO8601}][%p] %c - %m%n"));

	es.activateOptions();

	es.doAppend(new LoggingEvent("", null, System.currentTimeMillis(), Level.TRACE, "message1", null, null, null, null, null));

	Thread.sleep(1000);
	es.close();
  }
}
