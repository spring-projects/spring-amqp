package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.rabbit.test.RepeatProcessor;
import org.springframework.test.annotation.Repeat;

public class RabbitTemplatePerformanceIntegrationTests {

	private static final String ROUTE = "test.queue";

	private RabbitTemplate template = new RabbitTemplate();

	@Rule
	public RepeatProcessor repeat = new RepeatProcessor(4);

	@Rule
	// After the repeat processor, so it only runs once
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.ERROR, RabbitTemplate.class);

	@Rule
	// After the repeat processor, so it only runs once
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	private CachingConnectionFactory connectionFactory;

	@Before
	public void declareQueue() {
		if (repeat.isInitialized()) {
			// Important to prevent concurrent re-initialization
			return;
		}
		connectionFactory = new CachingConnectionFactory();
		connectionFactory.setChannelCacheSize(repeat.getConcurrency());
		// connectionFactory.setPort(5673);
		template.setConnectionFactory(connectionFactory);
		// TODO: investigate the effects of these flags...
		// template.setMandatoryPublish(true);
		// template.setImmediatePublish(true);
		RabbitAdmin admin = new RabbitAdmin(template);
		try {
			admin.deleteQueue(ROUTE);
		} catch (AmqpIOException e) {
			// Ignore (queue didn't exist)
		}
		admin.declareQueue(new Queue(ROUTE));
		admin.purgeQueue(ROUTE, false);
	}

	@After
	public void cleanUp() {
		if (repeat.isInitialized()) {
			return;
		}
		if (connectionFactory != null) {
			connectionFactory.destroy();
		}
	}

	@Test
	@Repeat(2000)
	public void testSendAndReceive() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		int count = 5;
		while (result == null && count-- > 0) {
			/*
			 * Retry for the purpose of non-transacted case because channel operations are async in that case
			 */
			Thread.sleep(10L);
			result = (String) template.receiveAndConvert(ROUTE);
		}
		assertEquals("message", result);
	}

	@Test
	@Repeat(2000)
	public void testSendAndReceiveTransacted() throws Exception {
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
	}

}
