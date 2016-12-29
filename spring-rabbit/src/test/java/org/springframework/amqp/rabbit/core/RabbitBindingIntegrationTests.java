/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.ActiveObjectCounter;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 */
public class RabbitBindingIntegrationTests {

	private static Queue queue = new Queue("test.queue");

	private CachingConnectionFactory connectionFactory;

	private RabbitTemplate template;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue.getName());

	@Before
	public void setup() {
		connectionFactory = new CachingConnectionFactory(BrokerTestUtils.getPort());
		connectionFactory.setHost("localhost");
		template = new RabbitTemplate(connectionFactory);
	}

	@After
	public void cleanUp() {
		this.brokerIsRunning.removeTestQueues();
		this.template.stop();
		this.connectionFactory.destroy();
	}

	@Test
	public void testSendAndReceiveWithTopicSingleCallback() throws Exception {

		final RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		final TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTag();
			assertNotNull(tag);

			template.convertAndSend("foo", "message");

			try {

				String result = getResult(consumer);
				assertEquals(null, result);

				template.convertAndSend("foo.end", "message");
				result = getResult(consumer);
				assertEquals("message", result);

			}
			finally {
				consumer.getChannel().basicCancel(tag);
			}

			return null;

		});
		admin.deleteExchange("topic");
	}

	@Test
	public void testSendAndReceiveWithNonDefaultExchange() throws Exception {

		final RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		final TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTag();
			assertNotNull(tag);

			template.convertAndSend("topic", "foo", "message");

			try {

				String result = getResult(consumer);
				assertEquals(null, result);

				template.convertAndSend("topic", "foo.end", "message");
				result = getResult(consumer);
				assertEquals("message", result);

			}
			finally {
				consumer.getChannel().basicCancel(tag);
			}

			return null;

		});
		admin.deleteExchange("topic");
	}

	@Test
	// @Ignore("Not sure yet if we need to support a use case like this")
	public void testSendAndReceiveWithTopicConsumeInBackground() throws Exception {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);
		template.setExchange(exchange.getName());

		BlockingQueueConsumer consumer = template.execute(channel -> {

			BlockingQueueConsumer consumer1 = createConsumer(template);
			String tag = consumer1.getConsumerTag();
			assertNotNull(tag);

			return consumer1;

		});

		template.convertAndSend("foo", "message");
		String result = getResult(consumer);
		assertEquals(null, result);

		template.convertAndSend("foo.end", "message");
		result = getResult(consumer);
		assertEquals("message", result);

		consumer.stop();
		admin.deleteExchange("topic");
		cachingConnectionFactory.destroy();

	}

	@Test
	public void testSendAndReceiveWithTopicTwoCallbacks() throws Exception {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTag();
			assertNotNull(tag);

			try {
				template.convertAndSend("foo", "message");
				String result = getResult(consumer);
				assertEquals(null, result);
			}
			finally {
				consumer.stop();
			}

			return null;

		});

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTag();
			assertNotNull(tag);

			try {
				template.convertAndSend("foo.end", "message");
				String result = getResult(consumer);
				assertEquals("message", result);
			}
			finally {
				consumer.stop();
			}

			return null;

		});
		admin.deleteExchange("topic");
	}

	@Test
	public void testSendAndReceiveWithFanout() throws Exception {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		FanoutExchange exchange = new FanoutExchange("fanout");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTag();
			assertNotNull(tag);

			try {
				template.convertAndSend("message");
				String result = getResult(consumer);
				assertEquals("message", result);
			}
			finally {
				consumer.stop();
			}

			return null;

		});
		admin.deleteExchange("fanout");
	}

	private BlockingQueueConsumer createConsumer(RabbitAccessor accessor) {
		BlockingQueueConsumer consumer = new BlockingQueueConsumer(
				accessor.getConnectionFactory(), new DefaultMessagePropertiesConverter(),
				new ActiveObjectCounter<BlockingQueueConsumer>(), AcknowledgeMode.AUTO, true, 1, queue.getName());
		consumer.start();
		// wait for consumeOk...
		int n = 0;
		while (n++ < 100) {
			if (consumer.getConsumerTag() == null) {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		}
		return consumer;
	}

	private String getResult(final BlockingQueueConsumer consumer) throws InterruptedException {
		Message response = consumer.nextMessage(2000L);
		if (response == null) {
			return null;
		}
		return (String) new SimpleMessageConverter().fromMessage(response);
	}

}
