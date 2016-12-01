/*
 * Copyright 2014-2016 the original author or authors.
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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.FixedReplyQueueDeadLetterTests.FixedReplyQueueDeadLetterConfig;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author Gary Russell
 * @since 1.3.6
 */

@ContextConfiguration(classes = FixedReplyQueueDeadLetterConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class FixedReplyQueueDeadLetterTests {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private DeadListener deadListener;

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunning();

	/**
	 * Sends a message to a service that upcases the String and returns as a reply
	 * using a {@link RabbitTemplate} configured with a fixed reply queue and
	 * reply listener, configured with JavaConfig. We expect the reply to time
	 * out and be sent to the DLQ.
	 * @throws Exception the exception.
	 */
	@Test
	public void test() throws Exception {
		assertNull(this.rabbitTemplate.convertSendAndReceive("foo"));
		assertTrue(this.deadListener.latch.await(10, TimeUnit.SECONDS));
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
			return new DirectExchange("dlx.test.requestEx", false, true);
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

		public void handleMessage(String foo) {
			latch.countDown();
		}

	}

}
