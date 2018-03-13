/*
 * Copyright 2002-2018 the original author or authors.
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Correlation;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.PendingConfirm;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannel.Listener;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannelImpl;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @author Gunar Hillert
 * @author Artem Bilan
 * @author Rolf Arne Corneliussen
 * @author Arnaud Cogoluègnes
 * @since 1.1
 *
 */
public class RabbitTemplatePublisherCallbacksIntegrationTests {

	private static final String ROUTE = "test.queue";

	private CachingConnectionFactory connectionFactory;

	private CachingConnectionFactory connectionFactoryWithConfirmsEnabled;

	private CachingConnectionFactory connectionFactoryWithReturnsEnabled;

	private RabbitTemplate templateWithConfirmsEnabled;

	private RabbitTemplate templateWithReturnsEnabled;

	@Before
	public void create() {
		connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(10);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsEnabled = new CachingConnectionFactory();
		connectionFactoryWithConfirmsEnabled.setHost("localhost");
		// When using publisher confirms, the cache size needs to be large enough
		// otherwise channels can be closed before confirms are received.
		connectionFactoryWithConfirmsEnabled.setChannelCacheSize(100);
		connectionFactoryWithConfirmsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsEnabled.setPublisherConfirms(true);
		templateWithConfirmsEnabled = new RabbitTemplate(connectionFactoryWithConfirmsEnabled);
		connectionFactoryWithReturnsEnabled = new CachingConnectionFactory();
		connectionFactoryWithReturnsEnabled.setHost("localhost");
		connectionFactoryWithReturnsEnabled.setChannelCacheSize(1);
		connectionFactoryWithReturnsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithReturnsEnabled.setPublisherReturns(true);
		templateWithReturnsEnabled = new RabbitTemplate(connectionFactoryWithReturnsEnabled);
	}

	@After
	public void cleanUp() {
		this.templateWithConfirmsEnabled.stop();
		this.templateWithReturnsEnabled.stop();
		this.connectionFactory.destroy();
		this.connectionFactoryWithConfirmsEnabled.destroy();
		this.connectionFactoryWithReturnsEnabled.destroy();
		this.brokerIsRunning.removeTestQueues();
	}

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	@Test
	public void testPublisherConfirmReceived() throws Exception {
		final CountDownLatch latch = new CountDownLatch(10000);
		final AtomicInteger acks = new AtomicInteger();
		final AtomicReference<CorrelationData> confirmCorrelation = new AtomicReference<CorrelationData>();
		this.templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> {
			acks.incrementAndGet();
			confirmCorrelation.set(correlationData);
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
			public Message postProcessMessage(Message message, Correlation correlation) {
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
		assertTrue(exec.awaitTermination(300, TimeUnit.SECONDS));
		assertTrue("" + latch.getCount(), latch.await(60, TimeUnit.SECONDS));
		assertTrue("" + mppLatch.getCount(), latch.await(60, TimeUnit.SECONDS));
		assertNotNull(confirmCorrelation.get());
		assertEquals("abc", confirmCorrelation.get().getId());
		assertNull(templateWithConfirmsEnabled.getUnconfirmed(-1));
		this.templateWithConfirmsEnabled.execute(channel -> {
			Map<?, ?> listenerMap = TestUtils.getPropertyValue(((ChannelProxy) channel).getTargetChannel(), "listenerForSeq",
					Map.class);
			int n = 0;
			while (n++ < 100 && listenerMap.size() > 0) {
				Thread.sleep(100);
			}
			assertEquals(0, listenerMap.size());
			return null;
		});

		Log logger = spy(TestUtils.getPropertyValue(connectionFactoryWithConfirmsEnabled, "logger", Log.class));
		new DirectFieldAccessor(connectionFactoryWithConfirmsEnabled).setPropertyValue("logger", logger);
		cleanUp();
		verify(logger, never()).error(any());
	}

	@Test
	public void testPublisherConfirmWithSendAndReceive() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<CorrelationData> confirmCD = new AtomicReference<CorrelationData>();
		templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> {
			confirmCD.set(correlationData);
			latch.countDown();
		});
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactoryWithConfirmsEnabled);
		container.setQueueNames(ROUTE);
		container.setMessageListener(
				new MessageListenerAdapter((ReplyingMessageListener<String, String>) String::toUpperCase));
		container.start();
		CorrelationData correlationData = new CorrelationData("abc");
		String result = (String) this.templateWithConfirmsEnabled.convertSendAndReceive(ROUTE, (Object) "message",
				correlationData);
		container.stop();
		assertEquals("MESSAGE", result);
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals(correlationData, confirmCD.get());
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
			catch (InterruptedException e) {
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
		assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		assertNull(templateWithConfirmsEnabled.getUnconfirmed(-1));
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
		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		assertNull(templateWithConfirmsEnabled.getUnconfirmed(-1));
		assertNull(secondTemplate.getUnconfirmed(-1));
	}

	@Test
	public void testPublisherReturns() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message> returns = new ArrayList<Message>();
		templateWithReturnsEnabled.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
			returns.add(message);
			latch.countDown();
		});
		templateWithReturnsEnabled.setMandatory(true);
		templateWithReturnsEnabled.convertAndSend(ROUTE + "junk", (Object) "message", new CorrelationData("abc"));
		assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
		assertEquals(1, returns.size());
		Message message = returns.get(0);
		assertEquals("message", new String(message.getBody(), "utf-8"));
	}

	@Test
	public void testPublisherReturnsWithMandatoryExpression() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message> returns = new ArrayList<Message>();
		templateWithReturnsEnabled.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
			returns.add(message);
			latch.countDown();
		});
		Expression mandatoryExpression = new SpelExpressionParser().parseExpression("'message'.bytes == body");
		templateWithReturnsEnabled.setMandatoryExpression(mandatoryExpression);
		templateWithReturnsEnabled.convertAndSend(ROUTE + "junk", (Object) "message", new CorrelationData("abc"));
		templateWithReturnsEnabled.convertAndSend(ROUTE + "junk", (Object) "foo", new CorrelationData("abc"));
		assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
		assertEquals(1, returns.size());
		Message message = returns.get(0);
		assertEquals("message", new String(message.getBody(), "utf-8"));
	}

	@Test
	public void testPublisherConfirmNotReceived() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		when(mockChannel.isOpen()).thenReturn(true);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		doReturn(new PublisherCallbackChannelImpl(mockChannel)).when(mockConnection).createChannel();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.set(true));
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		Thread.sleep(5);
		assertEquals(1, template.getUnconfirmedCount());
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(-1);
		assertEquals(0, template.getUnconfirmedCount());
		assertEquals(1, unconfirmed.size());
		assertEquals("abc", unconfirmed.iterator().next().getId());
		assertFalse(confirmed.get());
	}

	@Test
	public void testPublisherConfirmNotReceivedMultiThreads() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);
		when(mockChannel1.getNextPublishSeqNo()).thenReturn(1L, 2L, 3L, 4L);
		when(mockChannel2.getNextPublishSeqNo()).thenReturn(1L, 2L, 3L, 4L);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		PublisherCallbackChannelImpl channel1 = new PublisherCallbackChannelImpl(mockChannel1);
		PublisherCallbackChannelImpl channel2 = new PublisherCallbackChannelImpl(mockChannel2);
		when(mockConnection.createChannel()).thenReturn(channel1).thenReturn(channel2);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
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
			catch (InterruptedException e) {
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
		assertTrue(threadSentLatch.await(5, TimeUnit.SECONDS));
		assertEquals(2, template.getUnconfirmedCount());
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(-1);
		assertEquals(2, unconfirmed.size());
		assertEquals(0, template.getUnconfirmedCount());
		Set<String> ids = new HashSet<String>();
		Iterator<CorrelationData> iterator = unconfirmed.iterator();
		ids.add(iterator.next().getId());
		ids.add(iterator.next().getId());
		assertTrue(ids.remove("abc"));
		assertTrue(ids.remove("def"));
		assertFalse(confirmed.get());
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		Map<?, ?> pendingConfirms = (Map<?, ?>) dfa.getPropertyValue("publisherConfirmChannels");
		assertThat(pendingConfirms.size(), greaterThan(0)); // might use 2 or only 1 channel
		exec.shutdown();
		assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
		ccf.destroy();
		assertEquals(0, pendingConfirms.size());
	}

	@Test
	public void testPublisherConfirmNotReceivedAged() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		when(mockChannel.isOpen()).thenReturn(true);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		doReturn(new PublisherCallbackChannelImpl(mockChannel)).when(mockConnection).createChannel();

		final AtomicLong count = new AtomicLong();
		doAnswer(invocation -> count.incrementAndGet()).when(mockChannel).getNextPublishSeqNo();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.set(true));
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		Thread.sleep(100);
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		assertEquals(2, template.getUnconfirmedCount());
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(50);
		assertEquals(1, template.getUnconfirmedCount());
		assertEquals(1, unconfirmed.size());
		assertEquals("abc", unconfirmed.iterator().next().getId());
		assertFalse(confirmed.get());
		Thread.sleep(100);
		assertEquals(1, template.getUnconfirmedCount());
		assertEquals(1, unconfirmed.size());
		unconfirmed = template.getUnconfirmed(50);
		assertEquals(1, unconfirmed.size());
		assertEquals(0, template.getUnconfirmedCount());
		assertEquals("def", unconfirmed.iterator().next().getId());
		assertFalse(confirmed.get());
	}

	@Test
	public void testPublisherConfirmMultiple() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		when(mockChannel.isOpen()).thenReturn(true);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		PublisherCallbackChannelImpl callbackChannel = new PublisherCallbackChannelImpl(mockChannel);
		when(mockConnection.createChannel()).thenReturn(callbackChannel);

		final AtomicLong count = new AtomicLong();
		doAnswer(invocation -> count.incrementAndGet()).when(mockChannel).getNextPublishSeqNo();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
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
		assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(-1);
		assertNull(unconfirmed);
	}

	/**
	 * Tests that piggy-backed confirms (multiple=true) are distributed to the proper
	 * template.
	 * @throws Exception
	 */
	@Test
	public void testPublisherConfirmMultipleWithTwoListeners() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		when(mockChannel.isOpen()).thenReturn(true);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		PublisherCallbackChannelImpl callbackChannel = new PublisherCallbackChannelImpl(mockChannel);
		when(mockConnection.createChannel()).thenReturn(callbackChannel);

		final AtomicLong count = new AtomicLong();
		doAnswer(invocation -> count.incrementAndGet()).when(mockChannel).getNextPublishSeqNo();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
		final RabbitTemplate template1 = new RabbitTemplate(ccf);

		final Set<String> confirms = new HashSet<String>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		template1.setConfirmCallback((correlationData, ack, cause) -> {
			if (ack) {
				confirms.add(correlationData.getId() + "1");
				latch1.countDown();
			}
		});
		final RabbitTemplate template2 = new RabbitTemplate(ccf);

		final CountDownLatch latch2 = new CountDownLatch(1);
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
		assertTrue(latch1.await(1000, TimeUnit.MILLISECONDS));
		assertTrue(latch2.await(1000, TimeUnit.MILLISECONDS));
		Collection<CorrelationData> unconfirmed1 = template1.getUnconfirmed(-1);
		assertNull(unconfirmed1);
		Collection<CorrelationData> unconfirmed2 = template2.getUnconfirmed(-1);
		assertNull(unconfirmed2);
		assertTrue(confirms.contains("abc1"));
		assertTrue(confirms.contains("def2"));
		assertTrue(confirms.contains("ghi2"));
		assertEquals(3, confirms.size());
	}

	/**
	 * AMQP-262
	 * Sets up a situation where we are processing 'multi' acks at the same
	 * time as adding a new pending ack to the map. Test verifies we don't
	 * get a {@link ConcurrentModificationException}.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
	@Test
	public void testConcurrentConfirms() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		when(mockChannel.isOpen()).thenReturn(true);
		when(mockChannel.getNextPublishSeqNo()).thenReturn(1L, 2L, 3L, 4L);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		final PublisherCallbackChannelImpl channel = new PublisherCallbackChannelImpl(mockChannel);
		when(mockConnection.createChannel()).thenReturn(channel);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
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
				waitForAll3AcksLatch.countDown();
				acks.incrementAndGet();
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
		assertTrue(first2SentOnThread1Latch.await(10, TimeUnit.SECONDS));
		// there should be no concurrent execution exception here
		channel.handleAck(2, true);
		assertTrue(allSentLatch.await(10, TimeUnit.SECONDS));
		channel.handleAck(3, false);
		assertTrue(waitForAll3AcksLatch.await(10, TimeUnit.SECONDS));
		assertEquals(3, acks.get());


		channel.basicConsume("foo", false, (Map) null, null);
		verify(mockChannel).basicConsume("foo", false, (Map) null, null);

		channel.basicQos(3, false);
		verify(mockChannel).basicQos(3, false);

	}

	@Test
	public void testNackForBadExchange() throws Exception {
		final AtomicBoolean nack = new AtomicBoolean(true);
		final AtomicReference<CorrelationData> correlation = new AtomicReference<CorrelationData>();
		final AtomicReference<String> reason = new AtomicReference<String>();
		final CountDownLatch latch = new CountDownLatch(2);
		this.templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> {
			nack.set(ack);
			correlation.set(correlationData);
			reason.set(cause);
			latch.countDown();
		});
		Log logger = spy(TestUtils.getPropertyValue(connectionFactoryWithConfirmsEnabled, "logger", Log.class));
		final AtomicReference<String> log = new AtomicReference<String>();
		doAnswer(invocation -> {
			log.set((String) invocation.getArguments()[0]);
			invocation.callRealMethod();
			latch.countDown();
			return null;
		}).when(logger).error(any());
		new DirectFieldAccessor(connectionFactoryWithConfirmsEnabled).setPropertyValue("logger", logger);

		CorrelationData correlationData = new CorrelationData("bar");
		String exchange = UUID.randomUUID().toString();
		this.templateWithConfirmsEnabled.convertAndSend(exchange, "key", "foo", correlationData);
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertFalse(nack.get());
		assertEquals(correlationData.toString(), correlation.get().toString());
		assertThat(reason.get(), containsString("NOT_FOUND - no exchange '" + exchange));
		assertThat(log.get(), containsString("NOT_FOUND - no exchange '" + exchange));
	}

	@Test
	public void testConfirmReceivedAfterPublisherCallbackChannelScheduleClose() throws Exception {
		final CountDownLatch latch = new CountDownLatch(40);
		templateWithConfirmsEnabled.setConfirmCallback((correlationData, ack, cause) -> latch.countDown());

		ExecutorService executorService = Executors.newCachedThreadPool();
		for (int i = 0; i < 20; i++) {
			executorService.execute(() -> {
				templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
				templateWithConfirmsEnabled.convertAndSend("BAD_ROUTE", (Object) "bad", new CorrelationData("cba"));
			});
		}

		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertNull(templateWithConfirmsEnabled.getUnconfirmed(-1));
	}

	// AMQP-506 ConcurrentModificationException
	@Test
	public void testPublisherConfirmGetUnconfirmedConcurrency() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		when(mockChannel.isOpen()).thenReturn(true);
		final AtomicLong seq = new AtomicLong();
		doAnswer(invocation -> seq.incrementAndGet()).when(mockChannel).getNextPublishSeqNo();

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		doReturn(mockChannel).when(mockConnection).createChannel();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
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
		while (!sentAll.get() && System.currentTimeMillis() < t1 + 20000) {
			template.getUnconfirmed(-1);
		}
		assertTrue(sentAll.get());
		assertFalse(confirmed.get());
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
		 * The following will detect proper operation if any more calls are added in future.
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
		doAnswer(invocation -> seq1.incrementAndGet()).when(mockChannel1).getNextPublishSeqNo();

		Channel mockChannel2 = mock(Channel.class);
		when(mockChannel2.isOpen()).thenReturn(true);
		final AtomicLong seq2 = new AtomicLong();
		doAnswer(invocation -> seq2.incrementAndGet()).when(mockChannel2).getNextPublishSeqNo();

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel1, mockChannel2);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setPublisherConfirms(true);
		final RabbitTemplate template = new RabbitTemplate(ccf);

		final CountDownLatch confirmed = new CountDownLatch(1);
		template.setConfirmCallback((correlationData, ack, cause) -> confirmed.countDown());
		ExecutorService exec = Executors.newSingleThreadExecutor();
		final AtomicInteger sent = new AtomicInteger();
		doAnswer(invocation -> sent.incrementAndGet() < closeAfter).when(mockChannel1).isOpen();
		final CountDownLatch sentAll = new CountDownLatch(1);
		exec.execute(() -> {
			for (int i = 0; i < 1000; i++) {
				try {
					template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
				}
				catch (AmqpException e) { }
			}
			sentAll.countDown();
		});
		assertTrue(sentAll.await(10, TimeUnit.SECONDS));
		assertTrue(confirmed.await(10, TimeUnit.SECONDS));
	}

	@Test
	public void testPublisherCallbackChannelImplCloseWithPending() throws Exception {

		final AtomicInteger nacks = new AtomicInteger();

		Listener listener = mock(Listener.class);
		doAnswer(invocation -> {
			boolean ack = invocation.getArgument(1);
			if (!ack) {
				nacks.incrementAndGet();
			}
			return null;
		}).when(listener).handleConfirm(any(PendingConfirm.class), anyBoolean());
		when(listener.getUUID()).thenReturn(UUID.randomUUID().toString());
		when(listener.isConfirmListener()).thenReturn(true);

		Channel channelMock = mock(Channel.class);

		PublisherCallbackChannelImpl channel = new PublisherCallbackChannelImpl(channelMock);

		channel.addListener(listener);

		for (int i = 0; i < 2; i++) {
			long seq = i + 1000;
			channel.addPendingConfirm(listener, seq,
					new PendingConfirm(new CorrelationData(Long.toHexString(seq)), System.currentTimeMillis()));
		}

		channel.close();

		assertEquals(0, TestUtils.getPropertyValue(channel, "pendingConfirms", Map.class).size());
		assertEquals(2, nacks.get());

	}

}
