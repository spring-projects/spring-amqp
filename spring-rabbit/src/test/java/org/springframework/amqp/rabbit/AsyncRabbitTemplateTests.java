/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.amqp.rabbit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

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
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.GUnzipPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class AsyncRabbitTemplateTests {

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
		assertThat(mppCalled.get()).isTrue();
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
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount",
						Integer.class)).isEqualTo(2);
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
		assertThat(reply1.getMessageProperties().getConsumerQueue()).isEqualTo(Address.AMQ_RABBITMQ_REPLY_TO);
		Message reply2 = checkMessageResult(future2, "FOO");
		assertThat(reply2.getMessageProperties().getConsumerQueue()).isEqualTo(Address.AMQ_RABBITMQ_REPLY_TO);
		this.latch.set(null);
		waitForZeroInUseConsumers();
		assertThat(TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount",
						Integer.class)).isEqualTo(2);
		this.asyncDirectTemplate.stop();
		this.asyncDirectTemplate.start();
		assertThat(TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount",
						Integer.class)).isEqualTo(0);
	}

	private void waitForZeroInUseConsumers() throws InterruptedException {
		Map<?, ?> inUseConsumers = TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.inUseConsumerChannels", Map.class);
		await().until(() -> inUseConsumers.size() == 0);
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

	@SuppressWarnings("unchecked")
	@Test
	public void testCancel() {
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive("foo");
		future.cancel(false);
		assertThat(TestUtils.getPropertyValue(asyncTemplate, "pending", Map.class)).hasSize(0);
	}

	@Test
	public void testMessageCustomCorrelation() throws Exception {
		Message message = getFooMessage();
		message.getMessageProperties().setCorrelationId("foo");
		ListenableFuture<Message> future = this.asyncTemplate.sendAndReceive(message);
		Message result = checkMessageResult(future, "FOO");
		assertThat(result.getMessageProperties().getCorrelationId()).isEqualTo("foo");
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
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive(this.requests.getName() + "x",
				"foo");
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected exception");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause()).isInstanceOf(AmqpMessageReturnedException.class);
			assertThat(((AmqpMessageReturnedException) e.getCause()).getRoutingKey()).isEqualTo(this.requests.getName() + "x");
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
			assertThat(e.getCause()).isInstanceOf(AmqpMessageReturnedException.class);
			assertThat(((AmqpMessageReturnedException) e.getCause()).getRoutingKey()).isEqualTo(this.requests.getName() + "x");
		}
	}

	@Test
	@DirtiesContext
	public void testConvertWithConfirm() throws Exception {
		this.asyncTemplate.setEnableConfirms(true);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("sleep");
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull();
		assertThat(confirm.get(10, TimeUnit.SECONDS)).isTrue();
		checkConverterResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testMessageWithConfirm() throws Exception {
		this.asyncTemplate.setEnableConfirms(true);
		RabbitMessageFuture future = this.asyncTemplate
				.sendAndReceive(new SimpleMessageConverter().toMessage("sleep", new MessageProperties()));
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull();
		assertThat(confirm.get(10, TimeUnit.SECONDS)).isTrue();
		checkMessageResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testConvertWithConfirmDirect() throws Exception {
		this.asyncDirectTemplate.setEnableConfirms(true);
		RabbitConverterFuture<String> future = this.asyncDirectTemplate.convertSendAndReceive("sleep");
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull();
		assertThat(confirm.get(10, TimeUnit.SECONDS)).isTrue();
		checkConverterResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testMessageWithConfirmDirect() throws Exception {
		this.asyncDirectTemplate.setEnableConfirms(true);
		RabbitMessageFuture future = this.asyncDirectTemplate
				.sendAndReceive(new SimpleMessageConverter().toMessage("sleep", new MessageProperties()));
		ListenableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull();
		assertThat(confirm.get(10, TimeUnit.SECONDS)).isTrue();
		checkMessageResult(future, "SLEEP");
	}

	@SuppressWarnings("unchecked")
	@Test
	@DirtiesContext
	public void testReceiveTimeout() throws Exception {
		this.asyncTemplate.setReceiveTimeout(500);
		ListenableFuture<String> future = this.asyncTemplate.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(1);
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected ExecutionException");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause()).isInstanceOf(AmqpReplyTimeoutException.class);
		}
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(0);
		assertThat(callback.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(callback.ex).isInstanceOf(AmqpReplyTimeoutException.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	@DirtiesContext
	public void testReplyAfterReceiveTimeout() throws Exception {
		this.asyncTemplate.setReceiveTimeout(100);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("sleep");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(1);
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected ExecutionException");
		}
		catch (ExecutionException e) {
			assertThat(e.getCause()).isInstanceOf(AmqpReplyTimeoutException.class);
		}
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(0);
		assertThat(callback.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(callback.ex).isInstanceOf(AmqpReplyTimeoutException.class);

		/*
		 * Test there's no harm if the reply is received after the timeout. This
		 * is unlikely to happen because the future is removed from the pending
		 * map when it times out. However, there is a small race condition where
		 * the reply arrives at the same time as the timeout.
		 */
		future.set("foo");
		assertThat(callback.result).isNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	@DirtiesContext
	public void testStopCancelled() throws Exception {
		this.asyncTemplate.setReceiveTimeout(5000);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.addCallback(callback);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(1);
		this.asyncTemplate.stop();
		// Second stop() to be sure that it is idempotent
		this.asyncTemplate.stop();
		try {
			future.get(10, TimeUnit.SECONDS);
			fail("Expected CancellationException");
		}
		catch (CancellationException e) {
			assertThat(future.getNackCause()).isEqualTo("AsyncRabbitTemplate was stopped while waiting for reply");
		}
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(0);
		assertThat(callback.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(future.isCancelled()).isTrue();
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "taskScheduler")).isNull();

		/*
		 * Test there's no harm if the reply is received after the cancel. This
		 * should never happen because the container is stopped before canceling
		 * and the future is removed from the pending map.
		 */
		future.set("foo");
		assertThat(callback.result).isNull();
	}

	private void checkConverterResult(ListenableFuture<String> future, String expected) throws InterruptedException {
		final CountDownLatch cdl = new CountDownLatch(1);
		final AtomicReference<String> resultRef = new AtomicReference<>();
		future.addCallback(new ListenableFutureCallback<String>() {

			@Override
			public void onSuccess(String result) {
				resultRef.set(result);
				cdl.countDown();
			}

			@Override
			public void onFailure(Throwable ex) {
				cdl.countDown();
			}

		});
		assertThat(cdl.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(resultRef.get()).isEqualTo(expected);
	}

	private Message checkMessageResult(ListenableFuture<Message> future, String expected) throws InterruptedException {
		final CountDownLatch cdl = new CountDownLatch(1);
		final AtomicReference<Message> resultRef = new AtomicReference<>();
		future.addCallback(new ListenableFutureCallback<Message>() {

			@Override
			public void onSuccess(Message result) {
				resultRef.set(result);
				cdl.countDown();
			}

			@Override
			public void onFailure(Throwable ex) {
				cdl.countDown();
			}

		});
		assertThat(cdl.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(new String(resultRef.get().getBody())).isEqualTo(expected);
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
			connectionFactory.setPublisherConfirmType(ConfirmType.CORRELATED);
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
		public GZipPostProcessor gZipPostProcessor() {
			GZipPostProcessor gZipPostProcessor = new GZipPostProcessor();
			gZipPostProcessor.setCopyProperties(true);
			return gZipPostProcessor;
		}

		@Bean
		public RabbitTemplate template(ConnectionFactory connectionFactory) {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
			rabbitTemplate.setRoutingKey(requests().getName());
			rabbitTemplate.addBeforePublishPostProcessors(gZipPostProcessor());
			rabbitTemplate.addAfterReceivePostProcessors(new GUnzipPostProcessor());
			return rabbitTemplate;
		}

		@Bean
		public RabbitTemplate templateForDirect(ConnectionFactory connectionFactory) {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
			rabbitTemplate.setRoutingKey(requests().getName());
			rabbitTemplate.addBeforePublishPostProcessors(gZipPostProcessor());
			rabbitTemplate.addAfterReceivePostProcessors(new GUnzipPostProcessor());
			return rabbitTemplate;
		}

		@Bean
		@Primary
		public SimpleMessageListenerContainer replyContainer(ConnectionFactory connectionFactory) {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
			container.setAfterReceivePostProcessors(new GUnzipPostProcessor());
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
			container.setAfterReceivePostProcessors(new GUnzipPostProcessor());
			MessageListenerAdapter messageListener =
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
							});

			messageListener.setBeforeSendReplyPostProcessors(gZipPostProcessor());
			container.setMessageListener(messageListener);
			return container;
		}

	}

}
