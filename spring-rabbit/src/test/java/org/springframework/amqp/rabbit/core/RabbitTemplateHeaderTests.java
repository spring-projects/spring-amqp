/*
 * Copyright 2002-2016 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class RabbitTemplateHeaderTests {

	public static final String CORRELATION_HEADER = "spring_reply_correlation";

	@Test
	public void testNoExistingReplyToOrCorrelation() throws Exception {
		this.testNoExistingReplyToOrCorrelationGuts(true);
	}

	@Test
	public void testNoExistingReplyToOrCorrelationCustomKey() throws Exception {
		this.testNoExistingReplyToOrCorrelationGuts(false);
	}

	private void testNoExistingReplyToOrCorrelationGuts(final boolean standardHeader) throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		String replyAddress = "new.replyTo";
		template.setReplyAddress(replyAddress);
		template.expectedQueueNames();
		if (!standardHeader) {
			template.setCorrelationKey(CORRELATION_HEADER);
		}

		MessageProperties messageProperties = new MessageProperties();
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicReference<String> replyTo = new AtomicReference<String>();
		final AtomicReference<String> correlationId = new AtomicReference<String>();
		doAnswer(invocation -> {
			BasicProperties basicProps = (BasicProperties) invocation.getArguments()[3];
			replyTo.set(basicProps.getReplyTo());
			if (standardHeader) {
				correlationId.set(basicProps.getCorrelationId());
			}
			else {
				correlationId.set((String) basicProps.getHeaders().get(CORRELATION_HEADER));
			}
			MessageProperties springProps = new DefaultMessagePropertiesConverter()
					.toMessageProperties(basicProps, null, "UTF-8");
			Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
			template.onMessage(replyMessage);
			return null;
		}).when(mockChannel).basicPublish(Mockito.any(String.class), Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertNotNull(reply);

		assertNotNull(replyTo.get());
		assertEquals(replyAddress, replyTo.get());
		assertNotNull(correlationId.get());
		assertNull(reply.getMessageProperties().getReplyTo());
		if (standardHeader) {
			assertNull(reply.getMessageProperties().getCorrelationId());
		}
		else {
			assertNull(reply.getMessageProperties().getHeaders().get(CORRELATION_HEADER));
		}
	}

	@Test
	public void testReplyToOneDeep() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		String replyAddress = "new.replyTo";
		template.setReplyAddress(replyAddress);
		template.setReplyTimeout(60000);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setReplyTo("replyTo1");
		messageProperties.setCorrelationId("saveThis".getBytes());
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicReference<String> replyTo = new AtomicReference<String>();
		final AtomicReference<String> correlationId = new AtomicReference<String>();
		doAnswer(invocation -> {
			BasicProperties basicProps = (BasicProperties) invocation.getArguments()[3];
			replyTo.set(basicProps.getReplyTo());
			correlationId.set(basicProps.getCorrelationId());
			MessageProperties springProps = new DefaultMessagePropertiesConverter()
					.toMessageProperties(basicProps, null, "UTF-8");
			Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
			template.onMessage(replyMessage);
			return null;
		}).when(mockChannel).basicPublish(Mockito.any(String.class), Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertNotNull(reply);

		assertNotNull(replyTo.get());
		assertEquals(replyAddress, replyTo.get());
		assertNotNull(correlationId.get());
		assertFalse("saveThis".equals(correlationId.get()));
		assertEquals("replyTo1", reply.getMessageProperties().getReplyTo());

	}

	@Test
	public void testReplyToThreeDeep() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		String replyTo2 = "replyTo2";
		template.setReplyAddress(replyTo2);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		String replyTo1 = "replyTo1";
		messageProperties.setReplyTo(replyTo1);
		messageProperties.setCorrelationId("a".getBytes());
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicInteger count = new AtomicInteger();
		final List<String> nestedReplyTo = new ArrayList<String>();
		final List<String> nestedCorrelation = new ArrayList<String>();
		final String replyAddress3 = "replyTo3";
		doAnswer(invocation -> {
			BasicProperties basicProps = (BasicProperties) invocation.getArguments()[3];
			nestedReplyTo.add(basicProps.getReplyTo());
			nestedCorrelation.add(basicProps.getCorrelationId());
			MessageProperties springProps = new DefaultMessagePropertiesConverter()
					.toMessageProperties(basicProps, null, "UTF-8");
			Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
			if (count.incrementAndGet() < 2) {
				Message anotherMessage = new Message("Second".getBytes(), springProps);
				template.setReplyAddress(replyAddress3);
				replyMessage = template.sendAndReceive(anotherMessage);
				nestedReplyTo.add(replyMessage.getMessageProperties().getReplyTo());
				nestedCorrelation.add(new String(replyMessage.getMessageProperties().getCorrelationId(), "UTF-8"));
			}
			template.onMessage(replyMessage);
			return null;
		}).when(mockChannel).basicPublish(Mockito.any(String.class), Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertNotNull(reply);

		assertEquals(3, nestedReplyTo.size());
		assertEquals(replyTo2, nestedReplyTo.get(0));
		assertEquals(replyAddress3, nestedReplyTo.get(1));
		assertEquals(replyTo2, nestedReplyTo.get(2)); // intermediate reply

		assertEquals(replyTo1, reply.getMessageProperties().getReplyTo());
		assertEquals("a", new String(reply.getMessageProperties().getCorrelationId(), "UTF-8"));

	}

	@Test
	public void testReplyToOneDeepCustomCorrelationKey() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		template.setCorrelationKey(CORRELATION_HEADER);
		String replyAddress = "new.replyTo";
		template.setReplyAddress(replyAddress);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		String replyTo1 = "replyTo1";
		messageProperties.setReplyTo(replyTo1);
		messageProperties.setCorrelationId("saveThis".getBytes());
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicReference<String> replyTo = new AtomicReference<String>();
		final AtomicReference<String> correlationId = new AtomicReference<String>();
		doAnswer(invocation -> {
			BasicProperties basicProps = (BasicProperties) invocation.getArguments()[3];
			replyTo.set(basicProps.getReplyTo());
			correlationId.set((String) basicProps.getHeaders().get(CORRELATION_HEADER));

			MessageProperties springProps = new DefaultMessagePropertiesConverter()
					.toMessageProperties(basicProps, null, "UTF-8");
			Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
			template.onMessage(replyMessage);
			return null;
		}).when(mockChannel).basicPublish(Mockito.any(String.class), Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertNotNull(reply);

		assertNotNull(replyTo.get());
		assertEquals(replyAddress, replyTo.get());
		assertNotNull(correlationId.get());
		assertEquals(replyTo1, reply.getMessageProperties().getReplyTo());
		assertTrue(!"saveThis".equals(correlationId.get()));
		assertEquals(replyTo1, reply.getMessageProperties().getReplyTo());

	}

	@Test
	public void testReplyToThreeDeepCustomCorrelationKey() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		template.setCorrelationKey(CORRELATION_HEADER);
		String replyTo2 = "replyTo2";
		template.setReplyAddress(replyTo2);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		String replyTo1 = "replyTo1";
		messageProperties.setReplyTo(replyTo1);
		messageProperties.setCorrelationId("a".getBytes());
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicInteger count = new AtomicInteger();
		final List<String> nestedReplyTo = new ArrayList<String>();
		final List<String> nestedCorrelation = new ArrayList<String>();
		final String replyTo3 = "replyTo3";
		doAnswer(invocation -> {
			BasicProperties basicProps = (BasicProperties) invocation.getArguments()[3];
			nestedReplyTo.add(basicProps.getReplyTo());
			nestedCorrelation.add(basicProps.getCorrelationId());
			MessageProperties springProps = new DefaultMessagePropertiesConverter()
					.toMessageProperties(basicProps, null, "UTF-8");
			Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
			if (count.incrementAndGet() < 2) {
				Message anotherMessage = new Message("Second".getBytes(), springProps);
				template.setReplyAddress(replyTo3);
				replyMessage = template.sendAndReceive(anotherMessage);
				nestedReplyTo.add(replyMessage.getMessageProperties().getReplyTo());
				nestedCorrelation.add((String) replyMessage.getMessageProperties().getHeaders().get(CORRELATION_HEADER));
			}
			template.onMessage(replyMessage);
			return null;
		}).when(mockChannel).basicPublish(Mockito.any(String.class), Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertNotNull(reply);

		assertEquals(3, nestedReplyTo.size());
		assertEquals(replyTo2, nestedReplyTo.get(0));
		assertEquals(replyTo3, nestedReplyTo.get(1));
		assertEquals(replyTo2, nestedReplyTo.get(2)); //intermediate reply

		assertEquals(replyTo1, reply.getMessageProperties().getReplyTo());
		assertEquals("a", new String(reply.getMessageProperties().getCorrelationId(), "UTF-8"));

	}

}
