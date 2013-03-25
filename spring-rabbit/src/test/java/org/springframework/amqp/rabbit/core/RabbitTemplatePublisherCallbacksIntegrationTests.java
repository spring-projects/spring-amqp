/*
 * Copyright 2010-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannelImpl;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.DirectFieldAccessor;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

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
		connectionFactory.setChannelCacheSize(1);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsEnabled = new CachingConnectionFactory();
		// When using publisher confirms, the cache size needs to be large enough
		// otherwise channels can be closed before confirms are received.
		connectionFactoryWithConfirmsEnabled.setChannelCacheSize(10);
		connectionFactoryWithConfirmsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithConfirmsEnabled.setPublisherConfirms(true);
		templateWithConfirmsEnabled = new RabbitTemplate(connectionFactoryWithConfirmsEnabled);
		connectionFactoryWithReturnsEnabled = new CachingConnectionFactory();
		connectionFactoryWithReturnsEnabled.setChannelCacheSize(1);
		connectionFactoryWithReturnsEnabled.setPort(BrokerTestUtils.getPort());
		connectionFactoryWithReturnsEnabled.setPublisherReturns(true);
		templateWithReturnsEnabled = new RabbitTemplate(connectionFactoryWithReturnsEnabled);
	}

	@After
	public void cleanUp() {
		if (connectionFactory != null) {
			connectionFactory.destroy();
		}

		if (connectionFactoryWithConfirmsEnabled != null) {
			connectionFactoryWithConfirmsEnabled.destroy();
		}

		if (connectionFactoryWithReturnsEnabled != null) {
			connectionFactoryWithReturnsEnabled.destroy();
		}
	}

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	@Test
	public void testPublisherConfirmReceived() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		templateWithConfirmsEnabled.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch.countDown();
			}
		});
		templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
		assertNull(templateWithConfirmsEnabled.getUnconfirmed(0));
	}

	@Test
	public void testPublisherConfirmReceivedConcurrentThreads() throws Exception {
		final CountDownLatch latch = new CountDownLatch(2);
		templateWithConfirmsEnabled.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch.countDown();
			}
		});

		// Hold up the first thread so we get two channels
		final CountDownLatch threadLatch = new CountDownLatch(1);
		//Thread 1
		Executors.newSingleThreadExecutor().execute(new Runnable() {

			public void run() {
				templateWithConfirmsEnabled.execute(new ChannelCallback<Object>() {
					public Object doInRabbit(Channel channel) throws Exception {
						try {
							threadLatch.await(10, TimeUnit.SECONDS);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						templateWithConfirmsEnabled.doSend(channel, "", ROUTE,
							new SimpleMessageConverter().toMessage("message", new MessageProperties()),
							new CorrelationData("def"));
						return null;
					}
				});
			}
		});

		// Thread 2
		templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		threadLatch.countDown();
		assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		assertNull(templateWithConfirmsEnabled.getUnconfirmed(0));
	}

	@Test
	public void testPublisherConfirmReceivedTwoTemplates() throws Exception {
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		templateWithConfirmsEnabled.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch1.countDown();
			}
		});
		templateWithConfirmsEnabled.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		RabbitTemplate secondTemplate = new RabbitTemplate(connectionFactoryWithConfirmsEnabled);
		secondTemplate.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch2.countDown();
			}
		});
		secondTemplate.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		assertTrue(latch1.await(1000, TimeUnit.MILLISECONDS));
		assertTrue(latch2.await(1000, TimeUnit.MILLISECONDS));
		assertNull(templateWithConfirmsEnabled.getUnconfirmed(0));
		assertNull(secondTemplate.getUnconfirmed(0));
	}

	@Test
	public void testPublisherReturns() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message> returns = new ArrayList<Message>();
		templateWithReturnsEnabled.setReturnCallback(new ReturnCallback() {
			public void returnedMessage(Message message, int replyCode,
					String replyText, String exchange, String routingKey) {
				returns.add(message);
				latch.countDown();
			}
		});
		templateWithReturnsEnabled.setMandatory(true);
		templateWithReturnsEnabled.convertAndSend(ROUTE + "junk", (Object) "message", new CorrelationData("abc"));
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

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(new PublisherCallbackChannelImpl(mockChannel));

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				confirmed.set(true);
			}
		});
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		Thread.sleep(5);
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(0);
		assertEquals(1, unconfirmed.size());
		assertEquals("abc", unconfirmed.iterator().next().getId());
		assertFalse(confirmed.get());
	}

	@Test
	public void testPublisherConfirmNotReceivedMultiThreads() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		PublisherCallbackChannelImpl channel1 = new PublisherCallbackChannelImpl(mockChannel);
		PublisherCallbackChannelImpl channel2 = new PublisherCallbackChannelImpl(mockChannel);
		when(mockConnection.createChannel()).thenReturn(channel1).thenReturn(channel2);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				confirmed.set(true);
			}
		});

		// Hold up the first thread so we get two channels
		final CountDownLatch threadLatch = new CountDownLatch(1);
		final CountDownLatch threadSentLatch = new CountDownLatch(1);
		//Thread 1
		Executors.newSingleThreadExecutor().execute(new Runnable() {

			public void run() {
				template.execute(new ChannelCallback<Object>() {
					public Object doInRabbit(Channel channel) throws Exception {
						try {
							threadLatch.await(10, TimeUnit.SECONDS);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						template.doSend(channel, "", ROUTE,
							new SimpleMessageConverter().toMessage("message", new MessageProperties()),
							new CorrelationData("def"));
						threadSentLatch.countDown();
						return null;
					}
				});
			}
		});

		// Thread 2
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		threadLatch.countDown();
		assertTrue(threadSentLatch.await(5, TimeUnit.SECONDS));
		Thread.sleep(5);
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(0);
		assertEquals(2, unconfirmed.size());
		Set<String> ids = new HashSet<String>();
		Iterator<CorrelationData> iterator = unconfirmed.iterator();
		ids.add(iterator.next().getId());
		ids.add(iterator.next().getId());
		assertTrue(ids.remove("abc"));
		assertTrue(ids.remove("def"));
		assertFalse(confirmed.get());
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		Map<?, ?> pendingConfirms = (Map<?, ?>) dfa.getPropertyValue("pendingConfirms");
		assertEquals(2, pendingConfirms.size());
		channel1.close();
		assertEquals(1, pendingConfirms.size());
		channel2.close();
		assertEquals(0, pendingConfirms.size());
	}

	@Test
	public void testPublisherConfirmNotReceivedAged() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(new PublisherCallbackChannelImpl(mockChannel));

		final AtomicInteger count = new AtomicInteger();
		doAnswer(new Answer<Object>(){
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return count.incrementAndGet();
			}}).when(mockChannel).getNextPublishSeqNo();

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));

		final AtomicBoolean confirmed = new AtomicBoolean();
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				confirmed.set(true);
			}
		});
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		Thread.sleep(100);
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(50);
		assertEquals(1, unconfirmed.size());
		assertEquals("abc", unconfirmed.iterator().next().getId());
		assertFalse(confirmed.get());
		Thread.sleep(100);
		unconfirmed = template.getUnconfirmed(50);
		assertEquals(1, unconfirmed.size());
		assertEquals("def", unconfirmed.iterator().next().getId());
		assertFalse(confirmed.get());
	}

	@Test
	public void testPublisherConfirmMultiple() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		PublisherCallbackChannelImpl callbackChannel = new PublisherCallbackChannelImpl(mockChannel);
		when(mockConnection.createChannel()).thenReturn(callbackChannel);

		final AtomicInteger count = new AtomicInteger();
		doAnswer(new Answer<Object>(){
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return count.incrementAndGet();
			}}).when(mockChannel).getNextPublishSeqNo();

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));

		final List<String> confirms = new ArrayList<String>();
		final CountDownLatch latch = new CountDownLatch(2);
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				if (ack) {
					confirms.add(correlationData.getId());
					latch.countDown();
				}
			}
		});
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		callbackChannel.handleAck(2, true);
		assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
		Collection<CorrelationData> unconfirmed = template.getUnconfirmed(0);
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

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		PublisherCallbackChannelImpl callbackChannel = new PublisherCallbackChannelImpl(mockChannel);
		when(mockConnection.createChannel()).thenReturn(callbackChannel);

		final AtomicInteger count = new AtomicInteger();
		doAnswer(new Answer<Object>(){
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return count.incrementAndGet();
			}}).when(mockChannel).getNextPublishSeqNo();

		final RabbitTemplate template1 = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));

		final Set<String> confirms = new HashSet<String>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		template1.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				if (ack) {
					confirms.add(correlationData.getId() + "1");
					latch1.countDown();
				}
			}
		});
		final RabbitTemplate template2 = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));

		final CountDownLatch latch2 = new CountDownLatch(1);
		template2.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				if (ack) {
					confirms.add(correlationData.getId() + "2");
					latch2.countDown();
				}
			}
		});
		template1.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		template2.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		template2.convertAndSend(ROUTE, (Object) "message", new CorrelationData("ghi"));
		callbackChannel.handleAck(3, true);
		assertTrue(latch1.await(1000, TimeUnit.MILLISECONDS));
		assertTrue(latch2.await(1000, TimeUnit.MILLISECONDS));
		Collection<CorrelationData> unconfirmed1 = template1.getUnconfirmed(0);
		assertNull(unconfirmed1);
		Collection<CorrelationData> unconfirmed2 = template2.getUnconfirmed(0);
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
	@Test
	public void testConcurrentConfirms() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		when(mockChannel.getNextPublishSeqNo()).thenReturn(1L, 2L, 3L, 4L);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		final PublisherCallbackChannelImpl channel = new PublisherCallbackChannelImpl(mockChannel);
		when(mockConnection.createChannel()).thenReturn(channel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));

		final CountDownLatch first2SentOnThread1Latch = new CountDownLatch(1);
		final CountDownLatch delayAckProcessingLatch = new CountDownLatch(1);
		final CountDownLatch startedProcessingMultiAcksLatch = new CountDownLatch(1);
		final CountDownLatch waitForAll3AcksLatch = new CountDownLatch(3);
		final CountDownLatch allSentLatch = new CountDownLatch(1);
		final AtomicInteger acks = new AtomicInteger();
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
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
			}
		});
		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
				template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
				first2SentOnThread1Latch.countDown();
			}
		});
		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				try {
					startedProcessingMultiAcksLatch.await();
					template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("ghi"));
					allSentLatch.countDown();
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		assertTrue(first2SentOnThread1Latch.await(10, TimeUnit.SECONDS));
		// there should be no concurrent execution exception here
		channel.handleAck(2, true);
		assertTrue(allSentLatch.await(10, TimeUnit.SECONDS));
		channel.handleAck(3, false);
		assertTrue(waitForAll3AcksLatch.await(10, TimeUnit.SECONDS));
		assertEquals(3, acks.get());
	}
}
