/*
 * Copyright 2002-2012 the original author or authors.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.util.ReflectionUtils;

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

	@Test
	public void testPushPop() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		Method pushHeader = RabbitTemplate.class.getDeclaredMethod("pushHeaderValue", String.class, String.class);
		ReflectionUtils.makeAccessible(pushHeader);
		String header = (String) ReflectionUtils.invokeMethod(pushHeader, template, "a", null);
		assertEquals("a", header);
		header = (String) ReflectionUtils.invokeMethod(pushHeader, template, "b", header);
		assertEquals("b:a", header);
		header = (String) ReflectionUtils.invokeMethod(pushHeader, template, "c", header);
		assertEquals("c:b:a", header);

		Method popHeader = RabbitTemplate.class.getDeclaredMethod("popHeaderValue", String.class);
		ReflectionUtils.makeAccessible(popHeader);
		Object poppedHeader = ReflectionUtils.invokeMethod(popHeader, template, header);
		DirectFieldAccessor dfa = new DirectFieldAccessor(poppedHeader);
		assertEquals("c", dfa.getPropertyValue("poppedValue"));
		poppedHeader = ReflectionUtils.invokeMethod(popHeader, template, dfa.getPropertyValue("newValue"));
		dfa = new DirectFieldAccessor(poppedHeader);
		assertEquals("b", dfa.getPropertyValue("poppedValue"));
		poppedHeader = ReflectionUtils.invokeMethod(popHeader, template, dfa.getPropertyValue("newValue"));
		dfa = new DirectFieldAccessor(poppedHeader);
		assertEquals("a", dfa.getPropertyValue("poppedValue"));
		assertNull(dfa.getPropertyValue("newValue"));
	}

	@Test
	public void testReplyToOneDeep() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		Queue replyQueue = new Queue("new.replyTo");
		template.setReplyQueue(replyQueue);

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setReplyTo("replyTo1");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final List<BasicProperties> props = new ArrayList<BasicProperties>();
		doAnswer(new Answer<Object>() {
			public Object answer(InvocationOnMock invocation) throws Throwable {
				BasicProperties basicProps = (BasicProperties) invocation.getArguments()[4];
				props.add(basicProps);
				MessageProperties springProps = new DefaultMessagePropertiesConverter()
						.toMessageProperties(basicProps, null, "UTF-8");
				Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
				template.onMessage(replyMessage);
				return null;
			}}
		).when(mockChannel).basicPublish(Mockito.any(String.class),
				Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.anyBoolean(), Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);

		assertEquals(1, props.size());
		BasicProperties basicProperties = props.get(0);
		assertEquals("new.replyTo", basicProperties.getReplyTo());
		assertEquals("replyTo1", basicProperties.getHeaders().get(RabbitTemplate.STACKED_REPLY_TO_HEADER));
		assertNotNull(basicProperties.getHeaders().get(RabbitTemplate.STACKED_CORRELATION_HEADER));

	}

	@Test
	public void testReplyToTwoDeep() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		Queue replyQueue = new Queue("new.replyTo");
		template.setReplyQueue(replyQueue);

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setReplyTo("replyTo2");
		messageProperties.setHeader(RabbitTemplate.STACKED_REPLY_TO_HEADER, "replyTo1");
		messageProperties.setHeader(RabbitTemplate.STACKED_CORRELATION_HEADER, "a");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final List<BasicProperties> props = new ArrayList<BasicProperties>();
		doAnswer(new Answer<Object>() {
			public Object answer(InvocationOnMock invocation) throws Throwable {
				BasicProperties basicProps = (BasicProperties) invocation.getArguments()[4];
				props.add(basicProps);
				MessageProperties springProps = new DefaultMessagePropertiesConverter()
						.toMessageProperties(basicProps, null, "UTF-8");
				Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
				template.onMessage(replyMessage);
				return null;
			}}
		).when(mockChannel).basicPublish(Mockito.any(String.class),
				Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.anyBoolean(), Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);

		assertEquals(1, props.size());
		BasicProperties basicProperties = props.get(0);
		assertEquals("new.replyTo", basicProperties.getReplyTo());
		assertEquals("replyTo2:replyTo1", basicProperties.getHeaders().get(RabbitTemplate.STACKED_REPLY_TO_HEADER));
		assertTrue(((String)basicProperties.getHeaders().get(RabbitTemplate.STACKED_CORRELATION_HEADER)).endsWith(":a"));

		assertEquals("replyTo1", reply.getMessageProperties().getHeaders().get(RabbitTemplate.STACKED_REPLY_TO_HEADER));
		assertEquals("a", reply.getMessageProperties().getHeaders().get(RabbitTemplate.STACKED_CORRELATION_HEADER));
	}

	@Test
	public void testReplyToThreeDeep() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory(mockConnectionFactory));
		Queue replyQueue = new Queue("new.replyTo");
		template.setReplyQueue(replyQueue);

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setReplyTo("replyTo2");
		messageProperties.setHeader(RabbitTemplate.STACKED_REPLY_TO_HEADER, "replyTo1");
		messageProperties.setHeader(RabbitTemplate.STACKED_CORRELATION_HEADER, "a");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final List<BasicProperties> props = new ArrayList<BasicProperties>();
		final AtomicInteger count = new AtomicInteger();
		final List<String> nestedReplyTo = new ArrayList<String>();
		final List<String> nestedReplyStack = new ArrayList<String>();
		final List<String> nestedCorrelation = new ArrayList<String>();
		doAnswer(new Answer<Object>() {
			public Object answer(InvocationOnMock invocation) throws Throwable {
				BasicProperties basicProps = (BasicProperties) invocation.getArguments()[4];
				props.add(basicProps);
				MessageProperties springProps = new DefaultMessagePropertiesConverter()
						.toMessageProperties(basicProps, null, "UTF-8");
				Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
				if (count.incrementAndGet() < 2) {
					Message anotherMessage = new Message("Second".getBytes(), springProps);
					replyMessage = template.sendAndReceive(anotherMessage);
					nestedReplyTo.add(replyMessage.getMessageProperties().getReplyTo());
					nestedReplyStack.add((String) replyMessage
							.getMessageProperties().getHeaders()
							.get(RabbitTemplate.STACKED_REPLY_TO_HEADER));
					nestedCorrelation.add((String) replyMessage
							.getMessageProperties().getHeaders()
							.get(RabbitTemplate.STACKED_CORRELATION_HEADER));
				}
				template.onMessage(replyMessage);
				return null;
			}}
		).when(mockChannel).basicPublish(Mockito.any(String.class),
				Mockito.any(String.class), Mockito.anyBoolean(),
				Mockito.anyBoolean(), Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertNotNull(reply);

		assertEquals(2, props.size());
		BasicProperties basicProperties = props.get(0);
		assertEquals("new.replyTo", basicProperties.getReplyTo());
		assertEquals("replyTo2:replyTo1", basicProperties.getHeaders().get(RabbitTemplate.STACKED_REPLY_TO_HEADER));
		assertTrue(((String)basicProperties.getHeaders().get(RabbitTemplate.STACKED_CORRELATION_HEADER)).endsWith(":a"));

		basicProperties = props.get(1);
		assertEquals("new.replyTo", basicProperties.getReplyTo());
		assertEquals("new.replyTo:replyTo2:replyTo1", basicProperties.getHeaders().get(RabbitTemplate.STACKED_REPLY_TO_HEADER));
		assertTrue(((String)basicProperties.getHeaders().get(RabbitTemplate.STACKED_CORRELATION_HEADER)).endsWith(":a"));

		assertEquals("replyTo1", reply.getMessageProperties().getHeaders().get(RabbitTemplate.STACKED_REPLY_TO_HEADER));
		assertEquals("a", reply.getMessageProperties().getHeaders().get(RabbitTemplate.STACKED_CORRELATION_HEADER));

		assertEquals(1, nestedReplyTo.size());
		assertEquals(1, nestedReplyStack.size());
		assertEquals(1, nestedCorrelation.size());
		assertEquals("replyTo2:replyTo1", nestedReplyStack.get(0));
		assertTrue(nestedCorrelation.get(0).endsWith(":a"));

	}
}
