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

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.JavaConfigFixedReplyQueueTests.FixedReplyQueueConfig;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * <b>NOTE:</b> This class is referenced in the reference documentation; if it is changed/moved, be
 * sure to update that documentation.
 *
 * @author Gary Russell
 * @since 1.3
 */

@ContextConfiguration(classes = FixedReplyQueueConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class JavaConfigFixedReplyQueueTests {

	@Autowired
	private RabbitTemplate fixedReplyQRabbitTemplate;

	@Autowired
	private RabbitTemplate fixedReplyQRabbitTemplateNoReplyContainer;

	@Autowired
	private RabbitTemplate fixedReplyQRabbitTemplateWrongQueue;

	@Autowired
	private Exchange replyExchange;

	@Autowired
	private SimpleMessageListenerContainer replyListenerContainerWrongQueue;

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunning();

	/**
	 * Sends a message to a service that upcases the String and returns as a reply
	 * using a {@link RabbitTemplate} configured with a fixed reply queue and
	 * reply listener, configured with JavaConfig.
	 */
	@Test
	public void testReplyContainer() {
		assertEquals("FOO", this.fixedReplyQRabbitTemplate.convertSendAndReceive("foo"));
		Message message = MessageBuilder.withBody("foo".getBytes())
				.setContentType("text/plain")
				.build();
		Message reply = this.fixedReplyQRabbitTemplate.sendAndReceive(message);
		assertEquals(this.replyExchange.getName(), reply.getMessageProperties().getReceivedExchange());
	}

	@Test
	public void testReplyNoContainerNoTimeout() {
		try {
			this.fixedReplyQRabbitTemplateNoReplyContainer.convertSendAndReceive("foo");
			fail("expected exeption");
		}
		catch (IllegalStateException e) {
			assertThat(e.getMessage(),
					Matchers.containsString("RabbitTemplate is not configured as MessageListener - "
							+ "cannot use a 'replyAddress'"));
		}
	}

	@Test
	public void testMismatchedQueue() {
		try {
			this.replyListenerContainerWrongQueue.start();
			fail("expected exeption");
		}
		catch (UncategorizedAmqpException e) {
			Throwable t = e.getCause();
			assertThat(t, Matchers.instanceOf(IllegalStateException.class));
			assertThat(t.getMessage(),
					Matchers.containsString("Listener expects us to be listening on '["
							+ TestUtils.getPropertyValue(this.fixedReplyQRabbitTemplateWrongQueue, "replyAddress")
							+ "]'; our queues: " + Arrays.asList(this.replyListenerContainerWrongQueue.getQueueNames())));
		}
	}

	@Configuration
	public static class FixedReplyQueueConfig {

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost("localhost");
			return connectionFactory;
		}

		/**
		 * @return Rabbit template with fixed reply queue.
		 */
		@Bean
		public RabbitTemplate fixedReplyQRabbitTemplate() {
			RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory());
			template.setExchange(ex().getName());
			template.setRoutingKey("test");
			template.setReplyAddress(replyExchange().getName() + "/" + replyQueue().getName());
			return template;
		}

		/**
		 * @return Rabbit template with fixed reply queue, no reply container, no receive timeout.
		 */
		@Bean
		public RabbitTemplate fixedReplyQRabbitTemplateNoReplyContainer() {
			RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory());
			template.setExchange(ex().getName());
			template.setRoutingKey("testNoContainer");
			template.setReplyAddress(replyExchange().getName() + "/" + replyQueue().getName());
			return template;
		}

		/**
		 * @return Rabbit template with incorrect fixed reply queue.
		 */
		@Bean
		public RabbitTemplate fixedReplyQRabbitTemplateWrongQueue() {
			RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory());
			template.setExchange(ex().getName());
			template.setRoutingKey("test");
			template.setReplyAddress(requestQueue().getName());
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
		 * @return The reply listener container - the rabbit template with the wrong queue is the listener.
		 */
		@Bean
		public SimpleMessageListenerContainer replyListenerContainerWrongQueue() {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
			container.setConnectionFactory(rabbitConnectionFactory());
			container.setQueues(replyQueue());
			container.setMessageListener(fixedReplyQRabbitTemplateWrongQueue());
			container.setAutoStartup(false);
			return container;
		}

		/**
		 * @return a non-durable auto-delete exchange.
		 */
		@Bean
		public DirectExchange ex() {
			return new DirectExchange(UUID.randomUUID().toString(), false, true);
		}

		@Bean
		public Binding binding() {
			return BindingBuilder.bind(requestQueue()).to(ex()).with("test");
		}

		@Bean
		public Binding replyBinding() {
			return BindingBuilder.bind(replyQueue())
					.to(replyExchange())
					.with(replyQueue().getName());
		}

		/**
		 * @return an anonymous (auto-delete) queue.
		 */
		@Bean
		public Queue requestQueue() {
			return new AnonymousQueue();
		}

		@Bean
		public DirectExchange replyExchange() {
			return new DirectExchange(UUID.randomUUID().toString(), false, true);
		}

		/**
		 * @return an anonymous (auto-delete) queue.
		 */
		@Bean
		public Queue replyQueue() {
			return new AnonymousQueue();
		}

		/**
		 * @return an admin to handle the declarations.
		 */
		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(rabbitConnectionFactory());
		}

		/**
		 * Listener that upcases the request.
		 */
		public static class PojoListener {

			public String handleMessage(String foo) {
				return foo.toUpperCase();
			}
		}
	}

}
