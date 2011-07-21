package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertNull;

import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;

public class BlockingQueueConsumerIntegrationTests {
	
	private static Queue queue = new Queue("test.queue");

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
			BlockingQueueConsumerIntegrationTests.class);

	@Test
	public void testTransactionalLowLevel() throws Exception {

		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);

		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, queue.getName());
		blockingQueueConsumer.start();
		connectionFactory.destroy();

		// TODO: make this into a proper assertion. An exception can be thrown here by the Rabbit client and printed to
		// stderr without being rethrown (so hard to make a test fail).
		blockingQueueConsumer.stop();
		assertNull(template.receiveAndConvert(queue.getName()));

	}

}
