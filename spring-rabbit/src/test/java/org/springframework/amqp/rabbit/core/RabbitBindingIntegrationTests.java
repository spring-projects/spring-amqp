/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer;
import org.springframework.amqp.rabbit.support.ActiveObjectCounter;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 */
@RabbitAvailable(queues = RabbitBindingIntegrationTests.QUEUE_NAME)
public class RabbitBindingIntegrationTests {

	public static final String QUEUE_NAME = "test.queue.RabbitBindingIntegrationTests";

	private static Queue QUEUE = new Queue(QUEUE_NAME);

	private CachingConnectionFactory connectionFactory;

	private RabbitTemplate template;

	@BeforeEach
	public void setup() {
		connectionFactory = new CachingConnectionFactory(BrokerTestUtils.getPort());
		connectionFactory.setHost("localhost");
		template = new RabbitTemplate(connectionFactory);
	}

	@AfterEach
	public void cleanUp() {
		this.template.stop();
		this.connectionFactory.destroy();
	}

	@Test
	public void testSendAndReceiveWithTopicSingleCallback() {

		final RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		final TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(QUEUE).to(exchange).with("*.end"));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTags().iterator().next();
			assertThat(tag).isNotNull();

			template.convertAndSend("foo", "message");

			try {

				String result = getResult(consumer, false);
				assertThat(result).isEqualTo(null);

				template.convertAndSend("foo.end", "message");
				result = getResult(consumer, true);
				assertThat(result).isEqualTo("message");

			}
			finally {
				consumer.getChannel().basicCancel(tag);
			}

			return null;

		});
		admin.deleteExchange("topic");
	}

	@Test
	public void testSendAndReceiveWithNonDefaultExchange() {

		final RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		final TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);

		admin.declareBinding(BindingBuilder.bind(QUEUE).to(exchange).with("*.end"));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTags().iterator().next();
			assertThat(tag).isNotNull();

			template.convertAndSend("topic", "foo", "message");

			try {

				String result = getResult(consumer, false);
				assertThat(result).isEqualTo(null);

				template.convertAndSend("topic", "foo.end", "message");
				result = getResult(consumer, true);
				assertThat(result).isEqualTo("message");

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

		admin.declareBinding(BindingBuilder.bind(QUEUE).to(exchange).with("*.end"));

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template1 = new RabbitTemplate(cachingConnectionFactory);
		template1.setExchange(exchange.getName());

		BlockingQueueConsumer consumer = template1.execute(channel -> {

			BlockingQueueConsumer consumer1 = createConsumer(template1);
			String tag = consumer1.getConsumerTags().iterator().next();
			assertThat(tag).isNotNull();

			return consumer1;

		});

		template1.convertAndSend("foo", "message");
		String result = getResult(consumer, false);
		assertThat(result).isEqualTo(null);

		template1.convertAndSend("foo.end", "message");
		result = getResult(consumer, true);
		assertThat(result).isEqualTo("message");

		consumer.stop();
		admin.deleteExchange("topic");
		cachingConnectionFactory.destroy();

	}

	@Test
	public void testSendAndReceiveWithTopicTwoCallbacks() {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(QUEUE).to(exchange).with("*.end"));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTags().iterator().next();
			assertThat(tag).isNotNull();

			try {
				template.convertAndSend("foo", "message");
				String result = getResult(consumer, false);
				assertThat(result).isEqualTo(null);
			}
			finally {
				consumer.stop();
			}

			return null;

		});

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTags().iterator().next();
			assertThat(tag).isNotNull();

			try {
				template.convertAndSend("foo.end", "message");
				String result = getResult(consumer, true);
				assertThat(result).isEqualTo("message");
			}
			finally {
				consumer.stop();
			}

			return null;

		});
		admin.deleteExchange("topic");
	}

	@Test
	public void testSendAndReceiveWithFanout() {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		FanoutExchange exchange = new FanoutExchange("fanout");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(QUEUE).to(exchange));

		template.execute(channel -> {

			BlockingQueueConsumer consumer = createConsumer(template);
			String tag = consumer.getConsumerTags().iterator().next();
			assertThat(tag).isNotNull();

			try {
				template.convertAndSend("message");
				String result = getResult(consumer, true);
				assertThat(result).isEqualTo("message");
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
				new ActiveObjectCounter<BlockingQueueConsumer>(), AcknowledgeMode.AUTO, true, 1, QUEUE.getName());
		consumer.start();
		// wait for consumeOk...
		await().with().pollDelay(Duration.ZERO).until(() -> consumer.getConsumerTags().size() > 0);
		return consumer;
	}

	private String getResult(final BlockingQueueConsumer consumer, boolean expected) throws InterruptedException {
		Message response = consumer.nextMessage(expected ? 2000L : 100L);
		if (response == null) {
			return null;
		}
		return (String) new SimpleMessageConverter().fromMessage(response);
	}

}
