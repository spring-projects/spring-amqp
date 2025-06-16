/*
 * Copyright 2016-present the original author or authors.
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

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.AmqpReplyTimeoutException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.GUnzipPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ben Efrati
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

	@BeforeAll
	static void setup() {
		Awaitility.setDefaultTimeout(Duration.ofSeconds(30));
	}

	@Test
	public void testConvert1Arg() {
		final AtomicBoolean mppCalled = new AtomicBoolean();
		CompletableFuture<String> future = this.asyncTemplate.convertSendAndReceive("foo", m -> {
			mppCalled.set(true);
			return m;
		});
		checkConverterResult(future, "FOO");
		assertThat(mppCalled.get()).isTrue();
	}

	@Test
	public void testConvert1ArgDirect() {
		this.latch.set(new CountDownLatch(1));
		CompletableFuture<String> future1 = this.asyncDirectTemplate.convertSendAndReceive("foo");
		CompletableFuture<String> future2 = this.asyncDirectTemplate.convertSendAndReceive("bar");
		this.latch.get().countDown();
		checkConverterResult(future1, "FOO");
		checkConverterResult(future2, "BAR");
		this.latch.set(null);
		waitForZeroInUseConsumers();
		assertThat(TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount",
						AtomicInteger.class).get())
				.isEqualTo(2);
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
	public void testConvert2Args() {
		CompletableFuture<String> future = this.asyncTemplate.convertSendAndReceive(this.requests.getName(), "foo");
		checkConverterResult(future, "FOO");
	}

	@Test
	public void testConvert3Args() {
		CompletableFuture<String> future = this.asyncTemplate.convertSendAndReceive("", this.requests.getName(), "foo");
		checkConverterResult(future, "FOO");
	}

	@Test
	public void testConvert4Args() {
		CompletableFuture<String> future = this.asyncTemplate.convertSendAndReceive("", this.requests.getName(), "foo",
				message -> {
					String body = new String(message.getBody());
					return new Message((body + "bar").getBytes(), message.getMessageProperties());
				});
		checkConverterResult(future, "FOOBAR");
	}

	@Test
	public void testMessage1Arg() throws Exception {
		CompletableFuture<Message> future = this.asyncTemplate.sendAndReceive(getFooMessage());
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testMessage1ArgDirect() throws Exception {
		this.latch.set(new CountDownLatch(1));
		CompletableFuture<Message> future1 = this.asyncDirectTemplate.sendAndReceive(getFooMessage());
		CompletableFuture<Message> future2 = this.asyncDirectTemplate.sendAndReceive(getFooMessage());
		this.latch.get().countDown();
		Message reply1 = checkMessageResult(future1, "FOO");
		assertThat(reply1.getMessageProperties().getConsumerQueue()).isEqualTo(Address.AMQ_RABBITMQ_REPLY_TO);
		Message reply2 = checkMessageResult(future2, "FOO");
		assertThat(reply2.getMessageProperties().getConsumerQueue()).isEqualTo(Address.AMQ_RABBITMQ_REPLY_TO);
		this.latch.set(null);
		waitForZeroInUseConsumers();
		assertThat(TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount",
						AtomicInteger.class).get())
				.isEqualTo(2);
		this.asyncDirectTemplate.stop();
		this.asyncDirectTemplate.start();
		assertThat(TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.consumerCount",
						AtomicInteger.class).get())
				.isZero();
	}

	private void waitForZeroInUseConsumers() {
		Map<?, ?> inUseConsumers = TestUtils
				.getPropertyValue(this.asyncDirectTemplate, "directReplyToContainer.inUseConsumerChannels", Map.class);
		await().until(inUseConsumers::isEmpty);
	}

	@Test
	public void testMessage2Args() throws Exception {
		CompletableFuture<Message> future = this.asyncTemplate.sendAndReceive(this.requests.getName(), getFooMessage());
		checkMessageResult(future, "FOO");
	}

	@Test
	public void testMessage3Args() throws Exception {
		CompletableFuture<Message> future = this.asyncTemplate.sendAndReceive("", this.requests.getName(),
				getFooMessage());
		checkMessageResult(future, "FOO");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCancel() {
		CompletableFuture<String> future = this.asyncTemplate.convertSendAndReceive("foo");
		future.cancel(false);
		assertThat(TestUtils.getPropertyValue(asyncTemplate, "pending", Map.class)).isEmpty();
	}

	@Test
	public void testMessageCustomCorrelation() throws Exception {
		Message message = getFooMessage();
		message.getMessageProperties().setCorrelationId("foo");
		CompletableFuture<Message> future = this.asyncTemplate.sendAndReceive(message);
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
	public void testReturn() {
		this.asyncTemplate.setMandatory(true);
		CompletableFuture<String> future = this.asyncTemplate.convertSendAndReceive(this.requests.getName() + "x",
				"foo");
		assertThat(future)
				.as("Expected exception")
				.failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(ExecutionException.class)
				.havingCause()
				.isInstanceOf(AmqpMessageReturnedException.class)
				.extracting(cause -> ((AmqpMessageReturnedException) cause).getRoutingKey())
				.isEqualTo(this.requests.getName() + "x");
	}

	@Test
	@DirtiesContext
	public void testReturnDirect() {
		this.asyncDirectTemplate.setMandatory(true);
		CompletableFuture<String> future = this.asyncDirectTemplate.convertSendAndReceive(this.requests.getName() + "x",
				"foo");

		assertThat(future)
				.as("Expected exception")
				.failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(ExecutionException.class)
				.havingCause()
				.isInstanceOf(AmqpMessageReturnedException.class)
				.extracting(cause -> ((AmqpMessageReturnedException) cause).getRoutingKey())
				.isEqualTo(this.requests.getName() + "x");
	}

	@Test
	@DirtiesContext
	public void testConvertWithConfirm() {
		this.asyncTemplate.setEnableConfirms(true);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("sleep");
		CompletableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull()
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(true);
		checkConverterResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testMessageWithConfirm() throws Exception {
		this.asyncTemplate.setEnableConfirms(true);
		RabbitMessageFuture future = this.asyncTemplate
				.sendAndReceive(new SimpleMessageConverter().toMessage("sleep", new MessageProperties()));
		CompletableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull()
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(true);
		checkMessageResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testConvertWithConfirmDirect() {
		this.asyncDirectTemplate.setEnableConfirms(true);
		RabbitConverterFuture<String> future = this.asyncDirectTemplate.convertSendAndReceive("sleep");
		CompletableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull()
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(true);
		checkConverterResult(future, "SLEEP");
	}

	@Test
	@DirtiesContext
	public void testMessageWithConfirmDirect() throws Exception {
		this.asyncDirectTemplate.setEnableConfirms(true);
		RabbitMessageFuture future = this.asyncDirectTemplate
				.sendAndReceive(new SimpleMessageConverter().toMessage("sleep", new MessageProperties()));
		CompletableFuture<Boolean> confirm = future.getConfirm();
		assertThat(confirm).isNotNull()
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(true);
		checkMessageResult(future, "SLEEP");
	}

	@SuppressWarnings("unchecked")
	@Test
	@DirtiesContext
	public void testReceiveTimeout() throws Exception {
		this.asyncTemplate.setReceiveTimeout(500);
		CompletableFuture<String> future = this.asyncTemplate.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.whenComplete(callback);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(1);
		assertThat(future)
				.as("Expected ExecutionException")
				.failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(ExecutionException.class)
				.withCauseInstanceOf(AmqpReplyTimeoutException.class);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).isEmpty();
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
		future.whenComplete(callback);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(1);

		assertThat(future)
				.as("Expected ExecutionException")
				.failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(ExecutionException.class)
				.withCauseInstanceOf(AmqpReplyTimeoutException.class);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).isEmpty();
		assertThat(callback.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(callback.ex).isInstanceOf(AmqpReplyTimeoutException.class);

		/*
		 * Test there's no harm if the reply is received after the timeout. This
		 * is unlikely to happen because the future is removed from the pending
		 * map when it times out. However, there is a small race condition where
		 * the reply arrives at the same time as the timeout.
		 */
		future.complete("foo");
		assertThat(callback.result).isNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	@DirtiesContext
	public void testStopCancelled() throws Exception {
		this.asyncTemplate.setReceiveTimeout(5000);
		RabbitConverterFuture<String> future = this.asyncTemplate.convertSendAndReceive("noReply");
		TheCallback callback = new TheCallback();
		future.whenComplete(callback);
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).hasSize(1);
		this.asyncTemplate.stop();
		// Second stop() to be sure that it is idempotent
		this.asyncTemplate.stop();
		assertThat(future)
				.as("Expected CancellationException")
				.failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(CancellationException.class)
				.satisfies(e -> {
					assertThat(future.getNackCause()).isEqualTo("AsyncRabbitTemplate was stopped while waiting for reply");
					assertThat(future).isCancelled();
				});

		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "pending", Map.class)).isEmpty();
		assertThat(callback.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(TestUtils.getPropertyValue(this.asyncTemplate, "taskScheduler")).isNull();

		/*
		 * Test there's no harm if the reply is received after the cancel. This
		 * should never happen because the container is stopped before canceling
		 * and the future is removed from the pending map.
		 */
		future.complete("foo");
		assertThat(callback.result).isNull();
	}

	@Test
	@DirtiesContext
	public void testConversionException() {
		this.asyncTemplate.getRabbitTemplate().setMessageConverter(new SimpleMessageConverter() {

			@Override
			public Object fromMessage(Message message) throws MessageConversionException {
				throw new MessageConversionException("Failed to convert message");
			}
		});

		RabbitConverterFuture<String> replyFuture = this.asyncTemplate.convertSendAndReceive("conversionException");

		assertThat(replyFuture).failsWithin(Duration.ofSeconds(10))
				.withThrowableThat()
				.withCauseInstanceOf(MessageConversionException.class);
	}

	@Test
	void ctorCoverage() {
		AsyncRabbitTemplate template = new AsyncRabbitTemplate(mock(ConnectionFactory.class), "ex", "rk");
		assertThat(template).extracting(AsyncRabbitTemplate::getRabbitTemplate)
				.extracting("exchange")
				.isEqualTo("ex");
		assertThat(template).extracting(AsyncRabbitTemplate::getRabbitTemplate)
				.extracting("routingKey")
				.isEqualTo("rk");
		template = new AsyncRabbitTemplate(mock(ConnectionFactory.class), "ex", "rk", "rq");
		assertThat(template).extracting(AsyncRabbitTemplate::getRabbitTemplate)
				.extracting("exchange")
				.isEqualTo("ex");
		assertThat(template).extracting(AsyncRabbitTemplate::getRabbitTemplate)
				.extracting("routingKey")
				.isEqualTo("rk");
		assertThat(template)
				.extracting("replyAddress")
				.isEqualTo("rq");
		assertThat(template).extracting("container")
				.extracting("queueNames")
				.isEqualTo(new String[] {"rq"});
		template = new AsyncRabbitTemplate(mock(ConnectionFactory.class), "ex", "rk", "rq", "ra");
		assertThat(template).extracting(AsyncRabbitTemplate::getRabbitTemplate)
				.extracting("exchange")
				.isEqualTo("ex");
		assertThat(template).extracting(AsyncRabbitTemplate::getRabbitTemplate)
				.extracting("routingKey")
				.isEqualTo("rk");
		assertThat(template)
				.extracting("replyAddress")
				.isEqualTo("ra");
		assertThat(template).extracting("container")
				.extracting("queueNames")
				.isEqualTo(new String[] {"rq"});
		template = new AsyncRabbitTemplate(mock(RabbitTemplate.class), mock(AbstractMessageListenerContainer.class),
				"rq");
		assertThat(template)
				.extracting("replyAddress")
				.isEqualTo("rq");
	}

	@Test
	public void limitedChannelsAreReleasedOnTimeout() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		connectionFactory.setChannelCacheSize(1);
		connectionFactory.setChannelCheckoutTimeout(500L);
		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
		AsyncRabbitTemplate asyncRabbitTemplate = new AsyncRabbitTemplate(rabbitTemplate);
		asyncRabbitTemplate.setReceiveTimeout(500L);
		asyncRabbitTemplate.start();

		RabbitConverterFuture<String> replyFuture1 = asyncRabbitTemplate.convertSendAndReceive("noReply1");
		RabbitConverterFuture<String> replyFuture2 = asyncRabbitTemplate.convertSendAndReceive("noReply2");

		assertThatExceptionOfType(ExecutionException.class)
				.isThrownBy(() -> replyFuture1.get(10, TimeUnit.SECONDS))
				.withCauseInstanceOf(AmqpReplyTimeoutException.class);

		assertThatExceptionOfType(ExecutionException.class)
				.isThrownBy(() -> replyFuture2.get(10, TimeUnit.SECONDS))
				.withCauseInstanceOf(AmqpReplyTimeoutException.class);

		asyncRabbitTemplate.stop();
		connectionFactory.destroy();
	}

	private void checkConverterResult(CompletableFuture<String> future, String expected) {
		assertThat(future)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(expected);
	}

	private Message checkMessageResult(CompletableFuture<Message> future, String expected) throws InterruptedException {
		final CountDownLatch cdl = new CountDownLatch(1);
		final AtomicReference<Message> resultRef = new AtomicReference<>();
		future.whenComplete((result, ex) -> {
			resultRef.set(result);
			cdl.countDown();
		});
		assertThat(cdl.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(new String(resultRef.get().getBody())).isEqualTo(expected);
		await().untilAsserted(() ->
				assertThat(TestUtils.getPropertyValue(future, "timeoutTask", Future.class).isCancelled()).isTrue());
		return resultRef.get();
	}

	public static class TheCallback implements BiConsumer<String, Throwable> {

		private final CountDownLatch latch = new CountDownLatch(1);

		private volatile String result;

		private volatile Throwable ex;

		@Override
		public void accept(String result, Throwable ex) {
			this.result = result;
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
			container.setReceiveTimeout(10);
			container.setAfterReceivePostProcessors(new GUnzipPostProcessor());
			container.setQueueNames(replies().getName());
			return container;
		}

		@Bean
		public AsyncRabbitTemplate asyncTemplate(@Qualifier("template") RabbitTemplate template,
				SimpleMessageListenerContainer container) {

			return new AsyncRabbitTemplate(template, container);
		}

		@Bean
		public AsyncRabbitTemplate asyncDirectTemplate(
				@Qualifier("templateForDirect") RabbitTemplate templateForDirect) {

			return new AsyncRabbitTemplate(templateForDirect);
		}

		@Bean
		public SimpleMessageListenerContainer remoteContainer(ConnectionFactory connectionFactory) {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
			container.setReceiveTimeout(10);
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
