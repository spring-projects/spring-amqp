package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;

import com.rabbitmq.client.Channel;

public class CachingConnectionFactoryIntegrationTests {

	private CachingConnectionFactory connectionFactory = new CachingConnectionFactory();

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();
	
	@Rule
	public ExpectedException exception = ExpectedException.none();
	
	@Before
	public void open() {
		connectionFactory.setPort(BrokerTestUtils.getPort());
	}

	@After
	public void close() {
		// Release resources
		connectionFactory.reset();
	}

	@Test
	public void testSendAndReceiveFromVolatileQueue() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = admin.declareQueue();
		template.convertAndSend(queue.getName(), "message");
		String result = (String) template.receiveAndConvert(queue.getName());
		assertEquals("message", result);

	}

	@Test
	public void testSendAndReceiveFromVolatileQueueAfterImplicitRemoval() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = admin.declareQueue();
		template.convertAndSend(queue.getName(), "message");

		// Force a physical close of the channel
		connectionFactory.destroy();
		
		// The queue was removed when the channel was closed 
		exception.expect(AmqpIOException.class);

		String result = (String) template.receiveAndConvert(queue.getName());
		assertEquals("message", result);

	}

	@Test
	@Ignore // TODO: add this feature
	public void testMixTransactionalAndNonTransactional() throws Exception {

		RabbitTemplate template1 = new RabbitTemplate(connectionFactory);
		RabbitTemplate template2 = new RabbitTemplate(connectionFactory);
		template1.setChannelTransacted(true);

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = admin.declareQueue();

		template1.convertAndSend(queue.getName(), "message");
		String result = (String) template2.receiveAndConvert(queue.getName());
		assertEquals("message", result);
		
		// The channel is not transactional
		exception.expect(AmqpIOException.class);

		template2.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {
				// Should be an exception because the channel is not transactional
				channel.txRollback();
				return null;
			}
		});
		
	}

}
