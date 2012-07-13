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
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class RabbitTemplateTests {

	@Test
	public void returnConnectionAfterCommit() throws Exception {
		@SuppressWarnings("serial")
		TransactionTemplate txTemplate = new TransactionTemplate(new AbstractPlatformTransactionManager() {

			@Override
			protected Object doGetTransaction() throws TransactionException {
				return new Object();
			}

			@Override
			protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
			}

			@Override
			protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
			}

			@Override
			protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
			}
		});
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		when(mockChannel.isOpen()).thenReturn(true);

		final RabbitTemplate template = new RabbitTemplate(new CachingConnectionFactory(mockConnectionFactory));
		template.setChannelTransacted(true);

		txTemplate.execute(new TransactionCallback<Object>() {
			@Override
			public Object doInTransaction(TransactionStatus status) {
				template.convertAndSend("foo", "bar");
				return null;
			}
		});
		txTemplate.execute(new TransactionCallback<Object>() {
			@Override
			public Object doInTransaction(TransactionStatus status) {
				template.convertAndSend("baz", "qux");
				return null;
			}
		});
		verify(mockConnectionFactory, Mockito.times(1)).newConnection(Mockito.any(ExecutorService.class));
		// ensure we used the same channel
		verify(mockConnection, Mockito.times(1)).createChannel();
	}

	@Test
	public void testConvertbytes() {
		RabbitTemplate template = new RabbitTemplate();
		byte[] payload = "Hello, world!".getBytes();
		Message message = template.convertMessageIfNecessary(payload);
		assertSame(payload, message.getBody());
	}

	@Test
	public void testConvertString() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		String payload = "Hello, world!";
		Message message = template.convertMessageIfNecessary(payload);
		assertEquals(payload, new String(message.getBody(), SimpleMessageConverter.DEFAULT_CHARSET));
	}

	@Test
	public void testConvertSerializable() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		Long payload = 43L;
		Message message = template.convertMessageIfNecessary(payload);
		assertEquals(payload, SerializationUtils.deserialize(message.getBody()));
	}

	@Test
	public void testConvertMessage() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		Message input = new Message("Hello, world!".getBytes(), new MessageProperties());
		Message message = template.convertMessageIfNecessary(input);
		assertSame(input, message);
	}

}
