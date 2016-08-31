/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.AmqpReplyTimeoutException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitMessageFuture;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Gary Russell
 * @since 1.6
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class AsyncRabbitTemplateTests {

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunning();

	@Autowired
	private AsyncRabbitTemplate template;

	@Autowired
	private Queue requests;

	private final Message fooMessage = new SimpleMessageConverter().toMessage("foo", new MessageProperties());

	@Test
	public void testConvert1Arg() throws Exception {
		ListenableFuture<String> future = this.template.convertSendAndReceive("foo");
		checkConverterResult(future, "FOO");
	}

	@Test
	public void testConvert2Args() throws Exception {
		ListenableFuture<String> future = this.template.convertSendAndReceive(this.requests.getName(), "foo");
		checkConverterResult(future, "FOO");
	}

	@Test
	public void testConvert3Args() throws Exception {
		ListenableFuture<String> future = this.template.convertSendAndReceive("", this.requests.getName(), "foo");
		checkConverterResult(future, "FOO");
	}

	@Test
	public void testConvert4Args() throws Exception {
		ListenableFuture<String> future = this.template.convertSendAndReceive("", this.requests.getName(), "foo",
				message -> {
					String body = new String(message.getBody());
					return new Message((body + "bar").getBytes(), message.getMessageProperties());
				});
		checkConverterResult(future, "FOOBAR");
	}

	@Test
	public void testMessage1Arg() throws Exception {
		ListenableFuture<Message> future = this.template.sendAndReceive(this.fooMessage);
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testMessage2Args() throws Exception {
		ListenableFuture<Message> future = this.template.sendAndReceive(this.requests.getName(), this.fooMessage);
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testMessage3Args() throws Exception {
		ListenableFuture<Message> future = this.template.sendAndReceive("", this.requests.getName(), this.fooMessage);
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testCancel() throws Exception {
		ListenableFuture<String> future = this.template.convertSendAndReceive("foo");
		future.cancel(false);
		assertEquals(0, TestUtils.getPropertyValue(template, "pending", Map.class).size());
	}

	@Test
	public void testMessageCustomCorrelation() throws Exception {
		this.fooMessage.getMessageProperties().setCorrelationId("foo".getBytes());
		ListenableFuture<Message> future = this.template.sendAndReceive(this.fooMessage);
		Message result = checkMessageResult(future, "FOO");
		assertEquals("foo", new String(result.getMessageProperties().getCorrelationId()));
	}

	@Test
	@DirtiesContext
	public void testReturn() throws Exception {
		this.template.setMandatory(true);
		ListenableFuture<String> future = this.template.convertSendAndReceive(this.requests.getName() + "x", "foo");
		try {
			future.get();
		}
		catch (ExecutionException e) {
			assertThat(e.getCause(), instanceOf(AmqpMessageReturnedException.class));
			assertEquals(this.requests.getName() + "x", ((AmqpMessageReturnedException) e.getCause()).getRoutingKey());
		}
	}

	@Test
	@DirtiesContext
	public void testConvertWithConfirm() throws Exception {
		this.template.setEnableConfirms(true);
		RabbitConverterFuture<String> future = this.template.convertSendAndReceive("sleep");
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertNotNull(confirm);
		assertTrue(confirm.get(10, TimeUnit.SECONDS));
		checkConverterResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testMessageWithConfirm() throws Exception {
		this.template.setEnableConfirms(true);
		RabbitMessageFuture future = this.template
				.sendAndReceive(new SimpleMessageConverter().toMessage("sleep", new MessageProperties()));
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertNotNull(confirm);
		assertTrue(confirm.get(10, TimeUnit.SECONDS));
		checkMessageResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testReceiveTimeout() throws Exception {
		this.template.setReceiveTimeout(500);
		ListenableFuture<String> future = this.template.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertEquals(1, TestUtils.getPropertyValue(this.template, "pending", Map.class).size());
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected ExecutionException");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause(), instanceOf(AmqpReplyTimeoutException.class));
		}
		assertEquals(0, TestUtils.getPropertyValue(this.template, "pending", Map.class).size());
		assertTrue(callback.latch.await(10, TimeUnit.SECONDS));
		assertThat(callback.ex, instanceOf(AmqpReplyTimeoutException.class));
	}

	@Test
	@DirtiesContext
	public void testReplyAfterReceiveTimeout() throws Exception {
		this.template.setReceiveTimeout(100);
		RabbitConverterFuture<String> future = this.template.convertSendAndReceive("sleep");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertEquals(1, TestUtils.getPropertyValue(this.template, "pending", Map.class).size());
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected ExecutionException");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause(), instanceOf(AmqpReplyTimeoutException.class));
		}
		assertEquals(0, TestUtils.getPropertyValue(this.template, "pending", Map.class).size());
		assertTrue(callback.latch.await(10, TimeUnit.SECONDS));
		assertThat(callback.ex, instanceOf(AmqpReplyTimeoutException.class));

		/*
		 * Test there's no harm if the reply is received after the timeout. This
		 * is unlikely to happen because the future is removed from the pending
		 * map when it times out. However, there is a small race condition where
		 * the reply arrives at the same time as the timeout.
		 */
		future.set("foo");
		assertNull(callback.result);
	}

	@Test
	@DirtiesContext
	public void testStopCancelled() throws Exception {
		this.template.setReceiveTimeout(5000);
		RabbitConverterFuture<String> future = this.template.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertEquals(1, TestUtils.getPropertyValue(this.template, "pending", Map.class).size());
		this.template.stop();
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected CancellationException");
		}
		catch (CancellationException e) {
			assertEquals("AsyncRabbitTemplate was stopped while waiting for reply", future.getNackCause());
		}
		assertEquals(0, TestUtils.getPropertyValue(this.template, "pending", Map.class).size());
		assertTrue(callback.latch.await(10, TimeUnit.SECONDS));
		assertTrue(future.isCancelled());

		/*
		 * Test there's no harm if the reply is received after the cancel. This
		 * should never happen because the container is stopped before canceling
		 * and the future is removed from the pending map.
		 */
		future.set("foo");
		assertNull(callback.result);
	}

	private void checkConverterResult(ListenableFuture<String> future, String expected) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<String> resultRef = new AtomicReference<String>();
		future.addCallback(new ListenableFutureCallback<String>() {

			@Override
			public void onSuccess(String result) {
				resultRef.set(result);
				latch.countDown();
			}

			@Override
			public void onFailure(Throwable ex) {
				latch.countDown();
			}

		});
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals(expected, resultRef.get());
	}

	private Message checkMessageResult(ListenableFuture<Message> future, String expected) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Message> resultRef = new AtomicReference<Message>();
		future.addCallback(new ListenableFutureCallback<Message>() {

			@Override
			public void onSuccess(Message result) {
				resultRef.set(result);
				latch.countDown();
			}

			@Override
			public void onFailure(Throwable ex) {
				latch.countDown();
			}

		});
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals(expected, new String(resultRef.get().getBody()));
		return resultRef.get();
	}

	public static class TheCallback implements ListenableFutureCallback<String> {

		private final CountDownLatch latch = new CountDownLatch(1);

		private volatile String result;

		private volatile Throwable ex;

		@Override
		public void onSuccess(String result) {
			this.result = result;
			latch.countDown();
		}

		@Override
		public void onFailure(Throwable ex) {
			this.ex = ex;
			latch.countDown();
		}

	}

	@Configuration
	public static class Config {

		@Bean
		public ConnectionFactory connectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
			connectionFactory.setPublisherConfirms(true);
			connectionFactory.setPublisherReturns(true);
			return connectionFactory;
		}

		@Bean
		public Queue requests() {
			return new AnonymousQueue();
		}

		@Bean
		public Queue replies() {
			return new AnonymousQueue();
		}

		@Bean
		public RabbitAdmin admin(ConnectionFactory connectionFactory) {
			return new RabbitAdmin(connectionFactory);
		}

		@Bean
		public RabbitTemplate template(ConnectionFactory connectionFactory) {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
			rabbitTemplate.setRoutingKey(requests().getName());
			return rabbitTemplate;
		}

		@Bean
		@Primary
		public SimpleMessageListenerContainer replyContainer(ConnectionFactory connectionFactory) {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
			container.setQueueNames(replies().getName());
			return container;
		}

		@Bean
		public AsyncRabbitTemplate asyncTemplate(RabbitTemplate template, SimpleMessageListenerContainer container) {
			return new AsyncRabbitTemplate(template, container);
		}

		@Bean
		public SimpleMessageListenerContainer remoteContainer(ConnectionFactory connectionFactory) {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
			container.setQueueNames(requests().getName());
			container.setMessageListener(new MessageListenerAdapter(new Object() {

				@SuppressWarnings("unused")
				public String handleMessage(String message) {
					if ("sleep".equals(message)) {
						try {
							Thread.sleep(500); // time for confirm to be delivered, or timeout to occur
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
					else if ("noReply".equals(message)) {
						return null;
					}
					return message.toUpperCase();
				}

			}));
			return container;
		}

	}

}
