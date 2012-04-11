/*
 * Copyright 2010-2012 the original author or authors.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

	private RabbitTemplate template;

	@Before
	public void create() {
		connectionFactory = new CachingConnectionFactory();
		// When using publisher confirms, the cache size needs to be large enough
		// otherwise channels can be closed before confirms are received.
		connectionFactory.setChannelCacheSize(10);
		connectionFactory.setPort(BrokerTestUtils.getPort());
		connectionFactory.setPublisherConfirms(true);
		template = new RabbitTemplate(connectionFactory);
	}

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	@Test
	public void testPublisherConfirmReceived() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch.countDown();
			}
		});
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
		assertNull(template.getUnconfirmed(0));
	}

	@Test
	public void testPublisherConfirmReceivedConcurrentThreads() throws Exception {
		final CountDownLatch latch = new CountDownLatch(2);
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch.countDown();
			}
		});

		// Hold up the first thread so we get two channels
		final CountDownLatch threadLatch = new CountDownLatch(1);
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
						return null;
					}
				});
			}
		});

		// Thread 2
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		threadLatch.countDown();
		assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		assertNull(template.getUnconfirmed(0));
	}

	@Test
	public void testPublisherConfirmReceivedTwoTemplates() throws Exception {
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		template.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch1.countDown();
			}
		});
		template.convertAndSend(ROUTE, (Object) "message", new CorrelationData("abc"));
		RabbitTemplate secondTemplate = new RabbitTemplate(connectionFactory);
		secondTemplate.setConfirmCallback(new ConfirmCallback() {

			public void confirm(CorrelationData correlationData, boolean ack) {
				latch2.countDown();
			}
		});
		secondTemplate.convertAndSend(ROUTE, (Object) "message", new CorrelationData("def"));
		assertTrue(latch1.await(1000, TimeUnit.MILLISECONDS));
		assertTrue(latch2.await(1000, TimeUnit.MILLISECONDS));
		assertNull(template.getUnconfirmed(0));
		assertNull(secondTemplate.getUnconfirmed(0));
	}

	@Test
	public void testPublisherReturns() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message> returns = new ArrayList<Message>();
		template.setReturnCallback(new ReturnCallback() {
			public void returnedMessage(Message message, int replyCode,
					String replyText, String exchange, String routingKey) {
				returns.add(message);
				latch.countDown();
			}
		});
		template.setMandatory(true);
		template.setImmediate(true);
		template.convertAndSend(ROUTE + "junk", (Object) "message", new CorrelationData("abc"));
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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
}
