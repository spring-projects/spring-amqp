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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.logging.Log;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Correlation;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueBuilder.Overflow;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.PendingConfirm;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannel.Listener;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannelImpl;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Gunar Hillert
 * @author Artem Bilan
 * @author Rolf Arne Corneliussen
 * @author Arnaud Cogoluègnes
 * @since 1.1
 *
 */
@RabbitAvailable(queues = RabbitTemplatePublisherCallbacksIntegration1Tests.ROUTE)
public class RabbitTemplatePublisherCallbacksIntegration1Tests {

	public static final String ROUTE = "test.queue.RabbitTemplatePublisherCallbacksIntegrationTests";

	private static final ApplicationContext APPLICATION_CONTEXT = mock();

	private final ExecutorService executorService = Executors.newSingleThreadExecutor();

	private CachingConnectionFactory connectionFactory;

	private CachingConnectionFactory connectionFactoryWithConfirmsEnabled;

	private CachingConnectionFactory connectionFactoryWithReturnsEnabled;

	private CachingConnectionFactory connectionFactoryWithConfirmsAndReturnsEnabled;

	private RabbitTemplate templateWithConfirmsEnabled;

	private RabbitTemplate templateWithReturnsEnabled;

	private RabbitTemplate templateWithConfirmsAndReturnsEnabled;

	@BeforeEach
	public void create() {
		connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(10);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		connectionFactory.setApplicationContext(APPLICATION_CONTEXT);

		connectionFactoryWithConfirmsEnabled = new CachingConnectionFactory();
		connectionFactoryWithConfirmsEnabled.setHost("localhost");
		connectionFactoryWithConfirmsEnabled.setChannelCacheSize(100);
		connectionFactoryWithConfirmsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsEnabled.setPublisherConfirmType(ConfirmType.CORRELATED);
		connectionFactoryWithConfirmsEnabled.setApplicationContext(APPLICATION_CONTEXT);

		templateWithConfirmsEnabled = new RabbitTemplate(connectionFactoryWithConfirmsEnabled);

		connectionFactoryWithReturnsEnabled = new CachingConnectionFactory();
		connectionFactoryWithReturnsEnabled.setHost("localhost");
		connectionFactoryWithReturnsEnabled.setChannelCacheSize(1);
		connectionFactoryWithReturnsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithReturnsEnabled.setPublisherReturns(true);
		connectionFactoryWithReturnsEnabled.setApplicationContext(APPLICATION_CONTEXT);

		templateWithReturnsEnabled = new RabbitTemplate(connectionFactoryWithReturnsEnabled);
		templateWithReturnsEnabled.setMandatory(true);

		connectionFactoryWithConfirmsAndReturnsEnabled = new CachingConnectionFactory();
		connectionFactoryWithConfirmsAndReturnsEnabled.setHost("localhost");
		connectionFactoryWithConfirmsAndReturnsEnabled.setChannelCacheSize(100);
		connectionFactoryWithConfirmsAndReturnsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsAndReturnsEnabled.setPublisherConfirmType(ConfirmType.CORRELATED);
		connectionFactoryWithConfirmsAndReturnsEnabled.setPublisherReturns(true);
		connectionFactoryWithConfirmsAndReturnsEnabled.setApplicationContext(APPLICATION_CONTEXT);

		templateWithConfirmsAndReturnsEnabled = new RabbitTemplate(connectionFactoryWithConfirmsAndReturnsEnabled);
		templateWithConfirmsAndReturnsEnabled.setMandatory(true);
	}

	@AfterEach
	public void cleanUp() {
		this.templateWithConfirmsEnabled.stop();
		this.templateWithReturnsEnabled.stop();

		this.connectionFactory.onApplicationEvent(new ContextClosedEvent(APPLICATION_CONTEXT));
		this.connectionFactory.destroy();

		this.connectionFactoryWithConfirmsEnabled.onApplicationEvent(new ContextClosedEvent(APPLICATION_CONTEXT));
		this.connectionFactoryWithConfirmsEnabled.destroy();

		this.connectionFactoryWithReturnsEnabled.onApplicationEvent(new ContextClosedEvent(APPLICATION_CONTEXT));
		this.connectionFactoryWithReturnsEnabled.destroy();

		this.connectionFactoryWithConfirmsAndReturnsEnabled.onApplicationEvent(new ContextClosedEvent(APPLICATION_CONTEXT));
		this.connectionFactoryWithConfirmsAndReturnsEnabled.destroy();

		this.executorService.shutdown();
	}

	@Test
	public void testPublisherConfirmReceived() throws Exception {
		final CountDownLatch latch = new CountDownLatch(10000);
		final AtomicInteger acks = new AtomicInteger();
		final AtomicReference<CorrelationData> confirmCorrelation = new AtomicReference<>();
		AtomicReference<String> callbackThreadName = new AtomicReference<>();
		this.templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> {
			acks.incrementAndGet();
			confirmCorrelation.set(correlationData);
			callbackThreadName.set(Thread.currentThread().getName());
			latch.countDown();
		});
		this.templateWithConfirmsEnabled.setCorrelationDataPostProcessor((m, c) -> new CorrelationData("abc"));
		final CountDownLatch mppLatch = new CountDownLatch(10000);
		MessagePostProcessor mpp = new MessagePostProcessor() {

			@Override
			public Message postProcessMessage(Message message) throws AmqpException {
				return message;
			}

			@Override
			public Message postProcessMessage(Message message, Correlation correlation, String exch, String rk) {
				assertThat(exch).isEqualTo("");
				assertThat(rk).isEqualTo(ROUTE);
				mppLatch.countDown();
				return message;
			}

		};
		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i = 0; i < 100; i++) {
			exec.submit(() -> {
				try {
					for (int i1 = 0; i1 < 100; i1++) {
						templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", mpp);
					}
				}
				catch (Throwable t) {
					t.printStackTrace();
				}
			});
		}
		exec.shutdown();
		assertThat(exec.awaitTermination(300, TimeUnit.SECONDS)).isTrue();
		assertThat(mppLatch.await(300, TimeUnit.SECONDS)).as("" + mppLatch.getCount()).isTrue();
		assertThat(latch.await(300, TimeUnit.SECONDS)).as("" + latch.getCount()).isTrue();
		assertThat(confirmCorrelation.get()).isNotNull();
		assertThat(confirmCorrelation.get().getId()).isEqualTo("abc");
		assertThat(templateWithConfirmsEnabled.getUnconfirmed(-1)).isNull();
		this.templateWithConfirmsEnabled.execute(channel -> {
			Map<?, ?> listenerMap = TestUtils.getPropertyValue(((ChannelProxy) channel).getTargetChannel(),
					"listenerForSeq", Map.class);
			await().until(listenerMap::isEmpty);
			return null;
		});

		Log logger = spy(TestUtils.getPropertyValue(connectionFactoryWithConfirmsEnabled, "logger", Log.class));
		new DirectFieldAccessor(connectionFactoryWithConfirmsEnabled).setPropertyValue("logger", logger);
		cleanUp();
		verify(logger, never()).error(any());
		assertThat(callbackThreadName.get()).startsWith("spring-rabbit-deferred-pool");
	}

	@Test
	public void testPublisherConfirmWithSendAndReceive() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<CorrelationData> confirmCD = new AtomicReference<>();
		templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> {
			confirmCD.set(correlationData);
			latch.countDown();
		});
		SimpleMessageListenerContainer container =
				new SimpleMessageListenerContainer(this.connectionFactoryWithConfirmsEnabled);
		container.setQueueNames(ROUTE);
		container.setReceiveTimeout(10);
		container.setMessageListener(
				new MessageListenerAdapter((ReplyingMessageListener<String, String>) String::toUpperCase));
		container.start();
		CorrelationData correlationData = new CorrelationData("abc");
		String result = (String) this.templateWithConfirmsEnabled.convertSendAndReceive(ROUTE, (Object) "message",
				correlationData);
		container.stop();
		assertThat(result).isEqualTo("MESSAGE");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(confirmCD.get()).isEqualTo(correlationData);
	}

	@Test
	public void testPublisherConfirmReceivedConcurrentThreads() throws Exception {
		final CountDownLatch latch = new CountDownLatch(2);
		templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> latch.countDown());

		// Hold up the first thread so we get two channels
		final CountDownLatch threadLatch = new CountDownLatch(1);
		//Thread 1
		Executors.newSingleThreadExecutor().execute(() -> templateWithConfirmsEnabled.execute(channel -> {
			try {
				threadLatch.await(10, TimeUnit.SECONDS);
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			templateWithConfirmsEnabled.doSend(channel, "", ROUTE,
					new SimpleMessageConverter().toMessage("message", new MessageProperties()),
					false,
					new CorrelationData("def"));
			return null;
		}));

		// Thread 2
		templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		threadLatch.countDown();
		assertThat(latch.await(5000, TimeUnit.MILLISECONDS)).isTrue();
		assertThat(templateWithConfirmsEnabled.getUnconfirmed(-1)).isNull();
	}

	@Test
	public void testPublisherConfirmReceivedTwoTemplates() throws Exception {
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> latch1.countDown());
		templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		RabbitTemplate secondTemplate = new RabbitTemplate(connectionFactoryWithConfirmsEnabled);
		secondTemplate.setConfirmCallback((correlationData, ack, cause) -> latch2.countDown());
		secondTemplate.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(templateWithConfirmsEnabled.getUnconfirmed(-1)).isNull();
		assertThat(secondTemplate.getUnconfirmed(-1)).isNull();
	}

	@Test
	public void testPublisherReturns() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message> returns = new ArrayList<>();
		templateWithReturnsEnabled.setReturnsCallback((returned) -> {
			returns.add(returned.getMessage());
			latch.countDown();
		});
		templateWithReturnsEnabled.setMandatory(true);
		templateWithReturnsEnabled.convertAndSend(ROUTE + "junk", (Object) "message", new CorrelationData("abc"));
		assertThat(latch.await(1000, TimeUnit.MILLISECONDS)).isTrue();
		assertThat(returns).hasSize(1);
		Message message = returns.get(0);
		assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).isEqualTo("message");
	}

	@Test
	public void testPublisherReturnsWithMandatoryExpression() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message> returns = new ArrayList<>();
		templateWithReturnsEnabled.setReturnsCallback((returned) -> {
			returns.add(returned.getMessage());
			latch.countDown();
		});
		Expression mandatoryExpression = new SpelExpressionParser().parseExpression("'message'.bytes == body");
		templateWithReturnsEnabled.setMandatoryExpression(mandatoryExpression);
		templateWithReturnsEnabled.convertAndSend(ROUTE + "junk", (Object) "message", new CorrelationData("abc"));
		templateWithReturnsEnabled.convertAndSend(ROUTE + "junk", (Object) "foo", new CorrelationData("abc"));
		assertThat(latch.await(1000, TimeUnit.MILLISECONDS)).isTrue();
		assertThat(returns).hasSize(1);
		Message message = returns.get(0);
		assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).isEqualTo("message");
	}

	@Test
	public void testPublisherConfirmNotReceived() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockChannel.getNextPublishSeqNo()).willReturn(1L);

		CountDownLatch timeoutExceptionLatch = new CountDownLatch(1);

		given(mockChannel.waitForConfirms(anyLong()))
				.willAnswer(invocation -> {
					timeoutExceptionLatch.await(10, TimeUnit.SECONDS);
					throw new TimeoutException();
				});

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		willReturn(new PublisherCallbackChannelImpl(mockChannel, this.executorService))
				.given(mockConnection)
				.createChannel();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(Executors.newCachedThreadPool());
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10000);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.set(true));
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));

		assertThat(template.getUnconfirmedCount()).isEqualTo(1);
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(-1);
		assertThat(template.getUnconfirmedCount()).isEqualTo(0);
		assertThat(unconfirmed).hasSize(1);
		assertThat(unconfirmed.iterator().next().getId()).isEqualTo("abc");
		assertThat(confirmed.get()).isFalse();

		timeoutExceptionLatch.countDown();

		assertThat(ccf.createConnection().createChannel(false)).isNotNull();
	}

	@Test
	public void testPublisherConfirmNotReceivedMultiThreads() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		given(mockChannel1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);
		given(mockChannel1.getNextPublishSeqNo()).willReturn(1L, 2L, 3L, 4L);
		given(mockChannel2.getNextPublishSeqNo()).willReturn(1L, 2L, 3L, 4L);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		PublisherCallbackChannelImpl channel1 = new PublisherCallbackChannelImpl(mockChannel1, this.executorService);
		PublisherCallbackChannelImpl channel2 = new PublisherCallbackChannelImpl(mockChannel2, this.executorService);
		given(mockConnection.createChannel()).willReturn(channel1).willReturn(channel2);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.setChannelCacheSize(3);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.set(true));

		// Hold up the first thread so we get two channels
		final CountDownLatch threadLatch = new CountDownLatch(1);
		final CountDownLatch threadSentLatch = new CountDownLatch(1);
		//Thread 1
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.execute(() -> template.execute(channel -> {
			try {
				threadLatch.await(10, TimeUnit.SECONDS);
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			template.doSend(channel, "", ROUTE,
					new SimpleMessageConverter().toMessage("message", new MessageProperties()),
					false,
					new CorrelationData("def"));
			threadSentLatch.countDown();
			return null;
		}));

		// Thread 2
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc")); // channel y
		threadLatch.countDown();
		assertThat(threadSentLatch.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(template.getUnconfirmedCount()).isEqualTo(2);
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(-1);
		assertThat(unconfirmed).hasSize(2);
		assertThat(template.getUnconfirmedCount()).isEqualTo(0);
		Set<String> ids = new HashSet<String>();
		Iterator<CorrelationData> iterator = unconfirmed.iterator();
		ids.add(iterator.next().getId());
		ids.add(iterator.next().getId());
		assertThat(ids.remove("abc")).isTrue();
		assertThat(ids.remove("def")).isTrue();
		assertThat(confirmed.get()).isFalse();
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		Map<?, ?> pendingConfirms = (Map<?, ?>) dfa.getPropertyValue("publisherConfirmChannels");
		assertThat(pendingConfirms.size()).isGreaterThan(0); // might use 2 or only 1 channel
		exec.shutdown();
		assertThat(exec.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
		ccf.destroy();
		await().until(pendingConfirms::isEmpty);
	}

	@Test
	public void testPublisherConfirmNotReceivedAged() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		willReturn(new PublisherCallbackChannelImpl(mockChannel, this.executorService))
				.given(mockConnection)
				.createChannel();

		final AtomicLong count = new AtomicLong();
		willAnswer(invocation -> count.incrementAndGet()).given(mockChannel).getNextPublishSeqNo();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.set(true));
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		Thread.sleep(100);
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		assertThat(template.getUnconfirmedCount()).isEqualTo(2);
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(50);
		assertThat(template.getUnconfirmedCount()).isEqualTo(1);
		assertThat(unconfirmed).hasSize(1);
		assertThat(unconfirmed.iterator().next().getId()).isEqualTo("abc");
		assertThat(confirmed.get()).isFalse();
		Thread.sleep(100);
		assertThat(template.getUnconfirmedCount()).isEqualTo(1);
		assertThat(unconfirmed).hasSize(1);
		unconfirmed = template.getUnconfirmed(50);
		assertThat(unconfirmed).hasSize(1);
		assertThat(template.getUnconfirmedCount()).isEqualTo(0);
		assertThat(unconfirmed.iterator().next().getId()).isEqualTo("def");
		assertThat(confirmed.get()).isFalse();
	}

	@Test
	public void testPublisherConfirmMultiple() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		PublisherCallbackChannelImpl callbackChannel =
				new PublisherCallbackChannelImpl(mockChannel, this.executorService);
		given(mockConnection.createChannel()).willReturn(callbackChannel);

		final AtomicLong count = new AtomicLong();
		willAnswer(invocation -> count.incrementAndGet()).given(mockChannel).getNextPublishSeqNo();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final CountDownLatch latch = new CountDownLatch(2);
		template.setConfirmCallback((correlationData, ack, cause) -> {
			if (ack) {
				latch.countDown();
			}
		});
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		callbackChannel.handleAck(2, true);
		assertThat(latch.await(1000, TimeUnit.MILLISECONDS)).isTrue();
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(-1);
		assertThat(unconfirmed).isNull();
	}

	/**
	 * Tests that piggy-backed confirms (multiple=true) are distributed to the proper
	 * template.
	 */
	@Test
	public void testPublisherConfirmMultipleWithTwoListeners() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		PublisherCallbackChannelImpl callbackChannel =
				new PublisherCallbackChannelImpl(mockChannel, this.executorService);
		given(mockConnection.createChannel()).willReturn(callbackChannel);

		final AtomicLong count = new AtomicLong();
		willAnswer(invocation -> count.incrementAndGet()).given(mockChannel).getNextPublishSeqNo();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		final RabbitTemplate template1 = new RabbitTemplate(ccf);

		final Set<String> confirms = new HashSet<>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		template1.setConfirmCallback((correlationData, ack, cause) -> {
			if (ack) {
				confirms.add(correlationData.getId() + "1");
				latch1.countDown();
			}
		});
		final RabbitTemplate template2 = new RabbitTemplate(ccf);

		final CountDownLatch latch2 = new CountDownLatch(2);
		template2.setConfirmCallback((correlationData, ack, cause) -> {
			if (ack) {
				confirms.add(correlationData.getId() + "2");
				latch2.countDown();
			}
		});
		template1.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		template2.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		template2.convertAndSend(ROUTE, (Object) "message", new CorrelationData("ghi"));
		callbackChannel.handleAck(3, true);
		assertThat(latch1.await(1000, TimeUnit.MILLISECONDS)).isTrue();
		assertThat(latch2.await(1000, TimeUnit.MILLISECONDS)).isTrue();
		Collection<CorrelationData> unconfirmed1 = template1.getUnconfirmed(-1);
		assertThat(unconfirmed1).isNull();
		Collection<CorrelationData> unconfirmed2 = template2.getUnconfirmed(-1);
		assertThat(unconfirmed2).isNull();
		assertThat(confirms.contains("abc1")).isTrue();
		assertThat(confirms.contains("def2")).isTrue();
		assertThat(confirms.contains("ghi2")).isTrue();
		assertThat(confirms).hasSize(3);
	}

	/**
	 * AMQP-262
	 * Sets up a situation where we are processing 'multi' acks at the same
	 * time as adding a new pending ack to the map. Test verifies we don't
	 * get a {@link ConcurrentModificationException}.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testConcurrentConfirms() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockChannel.getNextPublishSeqNo()).willReturn(1L, 2L, 3L, 4L);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		final PublisherCallbackChannelImpl channel =
				new PublisherCallbackChannelImpl(mockChannel, this.executorService);
		given(mockConnection.createChannel()).willReturn(channel);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.setChannelCacheSize(3);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final CountDownLatch first2SentOnThread1Latch = new CountDownLatch(1);
		final CountDownLatch delayAckProcessingLatch = new CountDownLatch(1);
		final CountDownLatch startedProcessingMultiAcksLatch = new CountDownLatch(1);
		final CountDownLatch waitForAll3AcksLatch = new CountDownLatch(3);
		final CountDownLatch allSentLatch = new CountDownLatch(1);
		final AtomicInteger acks = new AtomicInteger();
		template.setConfirmCallback((correlationData, ack, cause) -> {
			try {
				startedProcessingMultiAcksLatch.countDown();
				// delay processing here; ensures thread 2 put would be concurrent
				delayAckProcessingLatch.await(2, TimeUnit.SECONDS);
				// only delay first time through
				delayAckProcessingLatch.countDown();
				acks.incrementAndGet();
				waitForAll3AcksLatch.countDown();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		Executors.newSingleThreadExecutor().execute(() -> {
			template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
			template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
			first2SentOnThread1Latch.countDown();
		});
		Executors.newSingleThreadExecutor().execute(() -> {
			try {
				startedProcessingMultiAcksLatch.await();
				template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("ghi"));
				allSentLatch.countDown();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		assertThat(first2SentOnThread1Latch.await(10, TimeUnit.SECONDS)).isTrue();
		// there should be no concurrent execution exception here
		channel.handleAck(2, true);
		assertThat(allSentLatch.await(10, TimeUnit.SECONDS)).isTrue();
		channel.handleAck(3, false);
		assertThat(waitForAll3AcksLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(acks.get()).isEqualTo(3);

		channel.basicConsume("foo", false, (Map) null, null);
		verify(mockChannel).basicConsume("foo", false, (Map) null, null);

		channel.basicQos(3, false);
		verify(mockChannel).basicQos(3, false);

	}

	@Test
	public void testNackForBadExchange() throws Exception {
		final AtomicBoolean nack = new AtomicBoolean(true);
		final AtomicReference<CorrelationData> correlation = new AtomicReference<>();
		final AtomicReference<String> reason = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(2);
		this.templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> {
			nack.set(ack);
			correlation.set(correlationData);
			reason.set(cause);
			latch.countDown();
		});
		Log logger = spy(TestUtils.getPropertyValue(connectionFactoryWithConfirmsEnabled, "logger", Log.class));
		final AtomicReference<String> log = new AtomicReference<>();
		willAnswer(invocation -> {
			log.set((String) invocation.getArguments()[0]);
			invocation.callRealMethod();
			latch.countDown();
			return null;
		}).given(logger).error(any());
		new DirectFieldAccessor(connectionFactoryWithConfirmsEnabled).setPropertyValue("logger", logger);

		CorrelationData correlationData = new CorrelationData("bar");
		String exchange = UUID.randomUUID().toString();
		this.templateWithConfirmsEnabled.convertAndSend(exchange, "key", "foo", correlationData);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(nack.get()).isFalse();
		assertThat(correlation.get().toString()).isEqualTo(correlationData.toString());
		assertThat(reason.get()).containsPattern(
				"(Channel closed by application|NOT_FOUND - no exchange '" + exchange + ")");
		assertThat(log.get()).contains("NOT_FOUND - no exchange '" + exchange);
	}

	@Test
	public void testConfirmReceivedAfterPublisherCallbackChannelScheduleClose() throws Exception {
		final CountDownLatch latch = new CountDownLatch(40);
		templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> latch.countDown());

		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i = 0; i < 20; i++) {
			exec.execute(() -> {
				templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
				templateWithConfirmsEnabled.convertAndSend("BAD_ROUTE", (Object) "bad", new CorrelationData("cba"));
			});
		}

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(templateWithConfirmsEnabled.getUnconfirmed(-1)).isNull();
		exec.shutdownNow();
	}

	// AMQP-506 ConcurrentModificationException
	@Test
	public void testPublisherConfirmGetUnconfirmedConcurrency() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);
		final AtomicLong seq = new AtomicLong();
		willAnswer(invocation -> seq.incrementAndGet()).given(mockChannel).getNextPublishSeqNo();

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		willReturn(mockChannel).given(mockConnection).createChannel();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.set(true));
		ExecutorService exec = Executors.newSingleThreadExecutor();
		final AtomicBoolean sentAll = new AtomicBoolean();
		exec.execute(() -> {
			for (int i = 0; i < 10000; i++) {
				template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
			}
			sentAll.set(true);
		});
		long t1 = System.currentTimeMillis();
		while (!sentAll.get() && System.currentTimeMillis() < t1 + 60_000) {
			template.getUnconfirmed(-1);
		}
		assertThat(sentAll.get()).isTrue();
		assertThat(confirmed.get()).isFalse();
		exec.shutdownNow();
	}

	@Test
	public void testPublisherConfirmCloseConcurrencyDetectInAllPlaces() throws Exception {
		/*
		 * For a new channel isOpen is currently called 5 times per send, addListener, setupConfirm x2, doSend,
		 * logicalClose (via closeChannel).
		 * For a cached channel an additional call is made in getChannel().
		 *
		 * Generally, there are currently three places (before invoke, logical close, get channel)
		 * where the close can be detected. Run the test to verify these (and any future calls
		 * that are added) properly emit the nacks.
		 *
		 * The following will detect proper operation if any more calls are added in the future.
		 */
		for (int i = 100; i < 110; i++) {
			testPublisherConfirmCloseConcurrency(i);
		}
	}

	// AMQP-532 ConcurrentModificationException
	/*
	 * closeAfter indicates when to return false from isOpen()
	 */
	private void testPublisherConfirmCloseConcurrency(final int closeAfter) throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		final AtomicLong seq1 = new AtomicLong();
		willAnswer(invocation -> seq1.incrementAndGet()).given(mockChannel1).getNextPublishSeqNo();

		Channel mockChannel2 = mock(Channel.class);
		given(mockChannel2.isOpen()).willReturn(true);
		final AtomicLong seq2 = new AtomicLong();
		willAnswer(invocation -> seq2.incrementAndGet()).given(mockChannel2).getNextPublishSeqNo();

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel1, mockChannel2);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(Executors.newSingleThreadExecutor());
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final CountDownLatch confirmed = new CountDownLatch(1);
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.countDown());
		ExecutorService exec = Executors.newSingleThreadExecutor();
		final AtomicInteger sent = new AtomicInteger();
		willAnswer(invocation -> sent.incrementAndGet() < closeAfter).given(mockChannel1).isOpen();
		final CountDownLatch sentAll = new CountDownLatch(1);
		exec.execute(() -> {
			template.invoke(t -> {
				for (int i = 0; i < 1000; i++) {
					try {
						t.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
					}
					catch (@SuppressWarnings("unused") AmqpException e) {

					}
				}
				return null;
			});
			sentAll.countDown();
		});
		assertThat(sentAll.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(confirmed.await(10, TimeUnit.SECONDS)).isTrue();
		exec.shutdownNow();
	}

	@Test
	public void testPublisherCallbackChannelImplCloseWithPending() throws Exception {

		Listener listener = mock(Listener.class);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {
			boolean ack = invocation.getArgument(1);
			if (!ack) {
				latch.countDown();
			}
			return null;
		}).given(listener).handleConfirm(any(PendingConfirm.class), anyBoolean());
		given(listener.getUUID()).willReturn(UUID.randomUUID().toString());
		given(listener.isConfirmListener()).willReturn(true);

		Channel channelMock = mock(Channel.class);

		PublisherCallbackChannelImpl channel = new PublisherCallbackChannelImpl(channelMock, this.executorService);

		channel.addListener(listener);

		for (int i = 0; i < 2; i++) {
			long seq = i + 1000;
			channel.addPendingConfirm(listener, seq,
					new PendingConfirm(new CorrelationData(Long.toHexString(seq)), System.currentTimeMillis()));
		}

		channel.close();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		Map<?, ?> pending = TestUtils.getPropertyValue(channel, "pendingConfirms", Map.class);
		await().until(pending::isEmpty);
	}

	@Test
	public void testWithFuture() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.connectionFactory);
		Queue queue = QueueBuilder.nonDurable()
				.autoDelete()
				.maxLength(1L)
				.overflow(Overflow.rejectPublish)
				.build();
		admin.declareQueue(queue);
		CorrelationData cd1 = new CorrelationData();
		this.templateWithConfirmsEnabled.convertAndSend("", queue.getName(), "foo", cd1);
		assertThat(cd1.getFuture().get(10, TimeUnit.SECONDS).isAck()).isTrue();
		CorrelationData cd2 = new CorrelationData();
		this.templateWithConfirmsEnabled.convertAndSend("", queue.getName(), "bar", cd2);
		assertThat(cd2.getFuture().get(10, TimeUnit.SECONDS).isAck()).isFalse();
		CorrelationData cd3 = new CorrelationData();
		this.templateWithConfirmsEnabled.convertAndSend("NO_EXCHANGE_HERE", queue.getName(), "foo", cd3);
		assertThat(cd3.getFuture().get(10, TimeUnit.SECONDS).isAck()).isFalse();
		assertThat(cd3.getFuture().get().getReason()).contains("NOT_FOUND");
		CorrelationData cd4 = new CorrelationData("42");
		AtomicBoolean resent = new AtomicBoolean();
		AtomicReference<String> callbackThreadName = new AtomicReference<>();
		CountDownLatch callbackLatch = new CountDownLatch(1);
		this.templateWithConfirmsAndReturnsEnabled.setReturnsCallback((returned) -> {
			this.templateWithConfirmsAndReturnsEnabled.send(ROUTE, returned.getMessage());
			callbackThreadName.set(Thread.currentThread().getName());
			resent.set(true);
			callbackLatch.countDown();
		});
		this.templateWithConfirmsAndReturnsEnabled.convertAndSend("", "NO_QUEUE_HERE", "foo", cd4);
		assertThat(cd4.getFuture().get(10, TimeUnit.SECONDS).isAck()).isTrue();
		assertThat(callbackLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(cd4.getReturned()).isNotNull();
		assertThat(resent.get()).isTrue();
		assertThat(callbackThreadName.get()).startsWith("spring-rabbit-deferred-pool");
		admin.deleteQueue(queue.getName());
	}

	@Test
	void justReturns() throws InterruptedException {
		CorrelationData correlationData = new CorrelationData();
		CountDownLatch latch = new CountDownLatch(1);
		this.templateWithReturnsEnabled.setReturnsCallback(returned -> {
			latch.countDown();
		});
		this.templateWithReturnsEnabled.setConfirmCallback((correlationData1, ack, cause) -> {
			// has callback but factory is not enabled
		});
		this.templateWithReturnsEnabled.convertAndSend("", ROUTE, "foo", correlationData);
		ChannelProxy channel = (ChannelProxy) this.connectionFactoryWithReturnsEnabled.createConnection()
				.createChannel(false);
		assertThat(channel.getTargetChannel())
				.extracting("pendingReturns")
				.asInstanceOf(InstanceOfAssertFactories.MAP)
				.isEmpty();
		assertThat(channel.getTargetChannel())
				.extracting("pendingConfirms")
				.asInstanceOf(InstanceOfAssertFactories.MAP)
				.extracting(map -> map.values().iterator().next())
				.asInstanceOf(InstanceOfAssertFactories.MAP)
				.isEmpty();

		this.templateWithReturnsEnabled.convertAndSend("", "___JUNK___", "foo", correlationData);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

}
