/*
 * Copyright 2014-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueBuilder.LeaderLocator;
import org.springframework.amqp.core.QueueBuilder.Overflow;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 *
 * @author Gary Russell
 * @since 1.3.6
 */

@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(management = true)
public class FixedReplyQueueDeadLetterTests extends NeedsManagementTests {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private DeadListener deadListener;

	@AfterAll
	static void tearDown() {
		brokerRunning.deleteQueues("all.args.1", "all.args.2", "all.args.3", "test.quorum");
	}

	/**
	 * Sends a message to a service that upcases the String and returns as a reply
	 * using a {@link RabbitTemplate} configured with a fixed reply queue and
	 * reply listener, configured with JavaConfig. We expect the reply to time
	 * out and be sent to the DLQ.
	 * @throws Exception the exception.
	 */
	@Test
	void test() throws Exception {
		assertThat(this.rabbitTemplate.convertSendAndReceive("foo")).isNull();
		assertThat(this.deadListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	void testQueueArgs1() throws MalformedURLException, URISyntaxException, InterruptedException {
		Map<String, Object> queue = await().until(() -> queueInfo("all.args.1"), que -> que != null);
		Map<String, Object> arguments = arguments(queue);
		assertThat(arguments.get("x-message-ttl")).isEqualTo(1000);
		assertThat(arguments.get("x-expires")).isEqualTo(200_000);
		assertThat(arguments.get("x-max-length")).isEqualTo(42);
		assertThat(arguments.get("x-max-length-bytes")).isEqualTo(10_000);
		assertThat(arguments.get("x-overflow")).isEqualTo("reject-publish");
		assertThat(arguments.get("x-dead-letter-exchange")).isEqualTo("reply.dlx");
		assertThat(arguments.get("x-dead-letter-routing-key")).isEqualTo("reply.dlrk");
		assertThat(arguments.get("x-max-priority")).isEqualTo(4);
		assertThat(arguments.get("x-queue-mode")).isEqualTo("lazy");
		assertThat(arguments.get(Queue.X_QUEUE_LEADER_LOCATOR)).isEqualTo(LeaderLocator.minLeaders.getValue());
		assertThat(arguments.get("x-single-active-consumer")).isEqualTo(Boolean.TRUE);
	}

	@Test
	void testQueueArgs2() throws MalformedURLException, URISyntaxException, InterruptedException {
		Map<String, Object> queue = await().until(() -> queueInfo("all.args.2"), que -> que != null);
		Map<String, Object> arguments = arguments(queue);
		assertThat(arguments.get("x-message-ttl")).isEqualTo(1000);
		assertThat(arguments.get("x-expires")).isEqualTo(200_000);
		assertThat(arguments.get("x-max-length")).isEqualTo(42);
		assertThat(arguments.get("x-max-length-bytes")).isEqualTo(10_000);
		assertThat(arguments.get("x-overflow")).isEqualTo("drop-head");
		assertThat(arguments.get("x-dead-letter-exchange")).isEqualTo("reply.dlx");
		assertThat(arguments.get("x-dead-letter-routing-key")).isEqualTo("reply.dlrk");
		assertThat(arguments.get("x-max-priority")).isEqualTo(4);
		assertThat(arguments.get("x-queue-mode")).isEqualTo("lazy");
		assertThat(arguments.get(Queue.X_QUEUE_LEADER_LOCATOR)).isEqualTo(LeaderLocator.clientLocal.getValue());
	}

	@Test
	void testQueueArgs3() throws URISyntaxException {
		Map<String, Object> queue = await().until(() -> queueInfo("all.args.3"), que -> que != null);
		Map<String, Object> arguments = arguments(queue);
		assertThat(arguments.get("x-message-ttl")).isEqualTo(1000);
		assertThat(arguments.get("x-expires")).isEqualTo(200_000);
		assertThat(arguments.get("x-max-length")).isEqualTo(42);
		assertThat(arguments.get("x-max-length-bytes")).isEqualTo(10_000);
		assertThat(arguments.get("x-overflow")).isEqualTo("reject-publish");
		assertThat(arguments.get("x-dead-letter-exchange")).isEqualTo("reply.dlx");
		assertThat(arguments.get("x-dead-letter-routing-key")).isEqualTo("reply.dlrk");
		assertThat(arguments.get("x-max-priority")).isEqualTo(4);
		assertThat(arguments.get("x-queue-mode")).isEqualTo("lazy");
		assertThat(arguments.get(Queue.X_QUEUE_LEADER_LOCATOR)).isEqualTo(LeaderLocator.random.getValue());

		Map<String, Object> exchange = exchangeInfo("dlx.test.requestEx");
		assertThat(arguments(exchange).get("alternate-exchange")).isEqualTo("alternate");
	}

	/*
	 * Does not require a 3.8 broker - they are just arbitrary arguments.
	 */
	@Test
	void testQuorumArgs() {
		Map<String, Object> queue = await().until(() -> queueInfo("test.quorum"), que -> que != null);
		Map<String, Object> arguments = arguments(queue);
		assertThat(arguments.get("x-queue-type")).isEqualTo("quorum");
		assertThat(arguments.get("x-delivery-limit")).isEqualTo(10);
	}

	@Configuration
	public static class FixedReplyQueueDeadLetterConfig {

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost("localhost");
			return connectionFactory;
		}

		/**
		 * @return Rabbit template with fixed reply queue and small timeout.
		 */
		@Bean
		public RabbitTemplate fixedReplyQRabbitTemplate() {
			RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory());
			template.setExchange(ex().getName());
			template.setRoutingKey("dlx.reply.test");
			template.setReplyAddress(replyQueue().getName());
			template.setReplyTimeout(1);
			return template;
		}

		/**
		 * @return The reply listener container - the rabbit template is the listener.
		 */
		@Bean
		public SimpleMessageListenerContainer replyListenerContainer() {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
			container.setConnectionFactory(rabbitConnectionFactory());
			container.setQueues(replyQueue());
			container.setMessageListener(fixedReplyQRabbitTemplate());
			return container;
		}

		/**
		 * @return The listener container that handles the request and returns the reply.
		 */
		@Bean
		public SimpleMessageListenerContainer serviceListenerContainer() {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
			container.setConnectionFactory(rabbitConnectionFactory());
			container.setQueues(requestQueue());
			container.setMessageListener(new MessageListenerAdapter(new PojoListener()));
			return container;
		}

		/**
		 * @return The listener container that handles the dead letter.
		 */
		@Bean
		public SimpleMessageListenerContainer dlListenerContainer() {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
			container.setConnectionFactory(rabbitConnectionFactory());
			container.setQueues(dlq());
			container.setMessageListener(new MessageListenerAdapter(deadListener()));
			return container;
		}

		/**
		 * @return a non-durable auto-delete exchange.
		 */
		@Bean
		public DirectExchange ex() {
			return ExchangeBuilder.directExchange("dlx.test.requestEx")
					.durable(false)
					.autoDelete()
					.alternate("alternate")
					.build();
		}

		@Bean
		public Binding binding() {
			return BindingBuilder.bind(requestQueue()).to(ex()).with("dlx.reply.test");
		}

		/**
		 * @return a non-durable auto-delete exchange.
		 */
		@Bean
		public DirectExchange dlx() {
			return new DirectExchange("reply.dlx", false, true);
		}

		/**
		 * @return an (auto-delete) queue.
		 */
		@Bean
		public Queue requestQueue() {
			return QueueBuilder.nonDurable("dlx.test.requestQ")
					.autoDelete()
					.build();
		}

		/**
		 * @return an (auto-delete) queue configured to go to the dlx.
		 */
		@Bean
		public Queue replyQueue() {
			return QueueBuilder.nonDurable("dlx.test.replyQ")
				    .autoDelete()
				    .withArgument("x-dead-letter-exchange", "reply.dlx")
				    .build();
		}

		/**
		 * @return an (auto-delete) queue.
		 */
		@Bean
		public Queue dlq() {
			return QueueBuilder.nonDurable("dlx.test.DLQ")
					.autoDelete()
					.build();
		}

		@Bean
		public Queue allArgs1() {
			return QueueBuilder.nonDurable("all.args.1")
					.ttl(1000)
					.expires(200_000)
					.maxLength(42)
					.maxLengthBytes(10_000)
					.overflow(Overflow.rejectPublish)
					.deadLetterExchange("reply.dlx")
					.deadLetterRoutingKey("reply.dlrk")
					.maxPriority(4)
					.lazy()
					.leaderLocator(LeaderLocator.minLeaders)
					.singleActiveConsumer()
					.build();
		}

		@Bean
		public Queue allArgs2() {
			return QueueBuilder.nonDurable("all.args.2")
					.ttl(1000)
					.expires(200_000)
					.maxLength(42)
					.maxLengthBytes(10_000)
					.overflow(Overflow.dropHead)
					.deadLetterExchange("reply.dlx")
					.deadLetterRoutingKey("reply.dlrk")
					.maxPriority(4)
					.lazy()
					.leaderLocator(LeaderLocator.clientLocal)
					.build();
		}

		@Bean
		public Queue allArgs3() {
			return QueueBuilder.nonDurable("all.args.3")
					.ttl(1000)
					.expires(200_000)
					.maxLength(42)
					.maxLengthBytes(10_000)
					.overflow(Overflow.rejectPublish)
					.deadLetterExchange("reply.dlx")
					.deadLetterRoutingKey("reply.dlrk")
					.maxPriority(4)
					.lazy()
					.leaderLocator(LeaderLocator.random)
					.build();
		}

		@Bean
		public Queue quorum() {
			return QueueBuilder.durable("test.quorum")
					.quorum()
					.deliveryLimit(10)
					.build();
		}

		@Bean
		public DeadListener deadListener() {
			return new DeadListener();
		}

		/**
		 * Bind the dlq to the dlx with the reply queue name.
		 * @return The binding.
		 */
		@Bean
		public Binding dlBinding() {
			return BindingBuilder.bind(dlq()).to(dlx()).with(replyQueue().getName());
		}

		/**
		 * @return an admin to handle the declarations.
		 */
		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(rabbitConnectionFactory());
		}

		/**
		 * Listener that upcases the request, but takes too long.
		 */
		public static class PojoListener {

			public String handleMessage(String foo) throws Exception {
				Thread.sleep(500);
				return foo.toUpperCase();
			}
		}

	}

	public static class DeadListener {
		private final CountDownLatch latch = new CountDownLatch(1);

		public void handleMessage(@SuppressWarnings("unused") String foo) {
			latch.countDown();
		}

	}

}
