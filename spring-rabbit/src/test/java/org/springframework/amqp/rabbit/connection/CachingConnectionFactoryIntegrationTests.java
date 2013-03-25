/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
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
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class CachingConnectionFactoryIntegrationTests {

	private static Log logger = LogFactory.getLog(CachingConnectionFactoryIntegrationTests.class);

	private CachingConnectionFactory connectionFactory;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void open() {
		connectionFactory = new CachingConnectionFactory();
		connectionFactory.setPort(BrokerTestUtils.getPort());
	}

	@After
	public void close() {
		connectionFactory.destroy();
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
	public void testReceiveFromNonExistentVirtualHost() throws Exception {

		connectionFactory.setVirtualHost("non-existent");
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		// Wrong vhost is very unfriendly to client - the exception has no clue (just an EOF)
		exception.expect(AmqpIOException.class);
		String result = (String) template.receiveAndConvert("foo");
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

	@Test
	public void testHardErrorAndReconnect() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = new Queue("foo");
		admin.declareQueue(queue);
		final String route = queue.getName();

		final CountDownLatch latch = new CountDownLatch(1);
		try {
			template.execute(new ChannelCallback<Object>() {
				public Object doInRabbit(Channel channel) throws Exception {
					channel.getConnection().addShutdownListener(new ShutdownListener() {
						public void shutdownCompleted(ShutdownSignalException cause) {
							logger.info("Error", cause);
							latch.countDown();
							// This will be thrown on the Connection thread just before it dies, so basically ignored
							throw new RuntimeException(cause);
						}
					});
					String tag = channel.basicConsume(route, new DefaultConsumer(channel));
					// Consume twice with the same tag is a hard error (connection will be reset)
					String result = channel.basicConsume(route, false, tag, new DefaultConsumer(channel));
					fail("Expected IOException, got: " + result);
					return null;
				}
			});
			fail("Expected AmqpIOException");
		} catch (AmqpIOException e) {
			// expected
		}
		template.convertAndSend(route, "message");
		assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
		String result = (String) template.receiveAndConvert(route);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(route);
		assertEquals(null, result);
	}

}
