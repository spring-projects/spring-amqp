/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.Address;
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
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
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
 * @author Artem Bilan
 *
 * @since 1.6
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class AsyncRabbitTemplateTests {

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunning();

	@Autowired
	private AsyncRabbitTemplate asyncTemplate;

	@Autowired
	private AsyncRabbitTemplate asyncDirectTemplate;

	@Autowired
	private Queue requests;

	@Autowired
	private AtomicReference<CountDownLatch> latch;

	private final Message fooMessage = new SimpleMessageConverter().toMessage("foo", new MessageProperties());

	@Test
	public void testConvert1Arg() throws Exception {
		final AtomicBoolean mppCalled = new AtomicBoolean();
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive("foo", m -> {
			mppCalled.set(true);
			return m;
		});
		checkConverterResult(future, "FOO");
		assertTrue(mppCalled.get());
	}

	@Test
	public void testConvert1ArgDirect() throws Exception {
		this.latch.set(new CountDownLatch(1));
		ListenableFuture<String> future1 = this.asyncDirectTemplate.convertSendAndReceive("foo");
		ListenableFuture<String> future2 = this.asyncDirectTemplate.convertSendAndReceive("bar");
		this.latch.get().countDown();
		checkConverterResult(future1, "FOO");
		checkConverterResult(future2, "BAR");
		this.latch.set(null);
		waitForZeroInUseConsumers();
		assertThat(TestUtils
						.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount", Integer.class),
				equalTo(2));
		final String missingQueue = UUID.randomUUID().toString();
		this.asyncDirectTemplate.convertSendAndReceive("", missingQueue, "foo"); // send to nowhere
		this.asyncDirectTemplate.stop(); // should clear the inUse channel map
		waitForZeroInUseConsumers();
		this.asyncDirectTemplate.start();
		this.asyncDirectTemplate.setReceiveTimeout(1);
		this.asyncDirectTemplate.convertSendAndReceive("", missingQueue, "foo"); // send to nowhere
		waitForZeroInUseConsumers();

		this.asyncDirectTemplate.setReceiveTimeout(10000);
		this.asyncDirectTemplate.convertSendAndReceive("", missingQueue, "foo").cancel(true);
		waitForZeroInUseConsumers();
	}

	@Test
	public void testConvert2Args() throws Exception {
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive(this.requests.getName(), "foo");
		checkConverterResult(future, "FOO");
	}

	@Test
	public void testConvert3Args() throws Exception {
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive("", this.requests.getName(), "foo");
		checkConverterResult(future, "FOO");
	}

	@Test
	public void testConvert4Args() throws Exception {
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive("", this.requests.getName(), "foo",
				message -> {
					String body = new String(message.getBody());
					return new Message((body + "bar").getBytes(), message.getMessageProperties());
				});
		checkConverterResult(future, "FOOBAR");
	}

	@Test
	public void testMessage1Arg() throws Exception {
		ListenableFuture<Message> future = this.asyncTemplate.sendAndReceive(getFooMessage());
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testMessage1ArgDirect() throws Exception {
		this.latch.set(new CountDownLatch(1));
		ListenableFuture<Message> future1 = this.asyncDirectTemplate.sendAndReceive(getFooMessage());
		ListenableFuture<Message> future2 = this.asyncDirectTemplate.sendAndReceive(getFooMessage());
		this.latch.get().countDown();
		Message reply1 = checkMessageResult(future1, "FOO");
		assertEquals(Address.AMQ_RABBITMQ_REPLY_TO, reply1.getMessageProperties().getConsumerQueue());
		Message reply2 = checkMessageResult(future2, "FOO");
		assertEquals(Address.AMQ_RABBITMQ_REPLY_TO, reply2.getMessageProperties().getConsumerQueue());
		this.latch.set(null);
		waitForZeroInUseConsumers();
		assertThat(TestUtils
					.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount", Integer.class),
				equalTo(2));
		this.asyncDirectTemplate.stop();
		this.asyncDirectTemplate.start();
		assertThat(TestUtils
					.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount", Integer.class),
				equalTo(0));
	}

	private void waitForZeroInUseConsumers() throws InterruptedException {
		int n = 0;
		Map<?, ?> inUseConsumers = TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.inUseConsumerChannels", Map.class);
		while (n++ < 100 && inUseConsumers.size() > 0) {
			Thread.sleep(100);
		}
		assertThat(inUseConsumers.size(), equalTo(0));
	}

	@Test
	public void testMessage2Args() throws Exception {
		ListenableFuture<Message> future = this.asyncTemplate.sendAndReceive(this.requests.getName(), getFooMessage());
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testMessage3Args() throws Exception {
		ListenableFuture<Message> future = this.asyncTemplate.sendAndReceive("", this.requests.getName(),
				getFooMessage());
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testCancel() throws Exception {
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive("foo");
		future.cancel(false);
		assertEquals(0, TestUtils.getPropertyValue(asyncTemplate, "pending", Map.class).size());
	}

	@Test
	public void testMessageCustomCorrelation() throws Exception {
		Message message = getFooMessage();
		message.getMessageProperties().setCorrelationId("foo");
		ListenableFuture<Message> future = this.asyncTemplate.sendAndReceive(message);
		Message result = checkMessageResult(future, "FOO");
		assertEquals("foo", result.getMessageProperties().getCorrelationId());
	}

	private Message getFooMessage() {
		this.fooMessage.getMessageProperties().setCorrelationId(null);
		this.fooMessage.getMessageProperties().setReplyTo(null);
		return this.fooMessage;
	}

	@Test
	@DirtiesContext
	public void testReturn() throws Exception {
		this.asyncTemplate.setMandatory(true);
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive(this.requests.getName() + "x", "foo");
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected exception");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause(), instanceOf(AmqpMessageReturnedException.class));
			assertEquals(this.requests.getName() + "x", ((AmqpMessageReturnedException) e.getCause()).getRoutingKey());
		}
	}

	@Test
	@DirtiesContext
	public void testReturnDirect() throws Exception {
		this.asyncDirectTemplate.setMandatory(true);
		ListenableFuture<String> future = this.asyncDirectTemplate.convertSendAndReceive(this.requests.getName() + "x",
				"foo");
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected exception");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause(), instanceOf(AmqpMessageReturnedException.class));
			assertEquals(this.requests.getName() + "x", ((AmqpMessageReturnedException) e.getCause()).getRoutingKey());
		}
	}

	@Test
	@DirtiesContext
	public void testConvertWithConfirm() throws Exception {
		this.asyncTemplate.setEnableConfirms(true);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("sleep");
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertNotNull(confirm);
		assertTrue(confirm.get(10, TimeUnit.SECONDS));
		checkConverterResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testMessageWithConfirm() throws Exception {
		this.asyncTemplate.setEnableConfirms(true);
		RabbitMessageFuture future = this.asyncTemplate
				.sendAndReceive(new SimpleMessageConverter().toMessage("sleep", new MessageProperties()));
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertNotNull(confirm);
		assertTrue(confirm.get(10, TimeUnit.SECONDS));
		checkMessageResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testConvertWithConfirmDirect() throws Exception {
		this.asyncDirectTemplate.setEnableConfirms(true);
		RabbitConverterFuture<String> future = this.asyncDirectTemplate.convertSendAndReceive("sleep");
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertNotNull(confirm);
		assertTrue(confirm.get(10, TimeUnit.SECONDS));
		checkConverterResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testMessageWithConfirmDirect() throws Exception {
		this.asyncDirectTemplate.setEnableConfirms(true);
		RabbitMessageFuture future = this.asyncDirectTemplate
				.sendAndReceive(new SimpleMessageConverter().toMessage("sleep", new MessageProperties()));
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertNotNull(confirm);
		assertTrue(confirm.get(10, TimeUnit.SECONDS));
		checkMessageResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testReceiveTimeout() throws Exception {
		this.asyncTemplate.setReceiveTimeout(500);
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertEquals(1, TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class).size());
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected ExecutionException");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause(), instanceOf(AmqpReplyTimeoutException.class));
		}
		assertEquals(0, TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class).size());
		assertTrue(callback.latch.await(10, TimeUnit.SECONDS));
		assertThat(callback.ex, instanceOf(AmqpReplyTimeoutException.class));
	}

	@Test
	@DirtiesContext
	public void testReplyAfterReceiveTimeout() throws Exception {
		this.asyncTemplate.setReceiveTimeout(100);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("sleep");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertEquals(1, TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class).size());
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected ExecutionException");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause(), instanceOf(AmqpReplyTimeoutException.class));
		}
		assertEquals(0, TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class).size());
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
		this.asyncTemplate.setReceiveTimeout(5000);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertEquals(1, TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class).size());
		this.asyncTemplate.stop();
		// Second stop() to be sure that it is idempotent
		this.asyncTemplate.stop();
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected CancellationException");
		}
		catch (CancellationException e) {
			assertEquals("AsyncRabbitTemplate was stopped while waiting for reply", future.getNackCause());
		}
		assertEquals(0, TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class).size());
		assertTrue(callback.latch.await(10, TimeUnit.SECONDS));
		assertTrue(future.isCancelled());
		assertNull(TestUtils.getPropertyValue(this.asyncTemplate, "taskScheduler"));

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
		public AtomicReference<CountDownLatch> latch() {
			return new AtomicReference<>();
		}

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
		public RabbitTemplate templateForDirect(ConnectionFactory connectionFactory) {
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
		public AsyncRabbitTemplate asyncDirectTemplate(RabbitTemplate templateForDirect) {
			return new AsyncRabbitTemplate(templateForDirect);
		}

		@Bean
		public SimpleMessageListenerContainer remoteContainer(ConnectionFactory connectionFactory) {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
			container.setQueueNames(requests().getName());
			container.setMessageListener(
					new MessageListenerAdapter((ReplyingMessageListener<String, String>)
							message -> {
								CountDownLatch countDownLatch = latch().get();
								if (countDownLatch != null) {
									try {
										countDownLatch.await(10, TimeUnit.SECONDS);
									}
									catch (InterruptedException e) {
										Thread.currentThread().interrupt();
									}
								}
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
							}));
			return container;
		}

	}

}
