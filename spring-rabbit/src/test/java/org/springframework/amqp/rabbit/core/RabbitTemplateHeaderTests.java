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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

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

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
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
		willAnswer(invocation -> {
			BasicProperties basicProps = invocation.getArgument(3);
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
			template.onMessage(replyMessage, mock(Channel.class));
			return null;
		}).given(mockChannel).basicPublish(any(String.class), any(String.class), Mockito.anyBoolean(),
				any(BasicProperties.class), any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertThat(reply).isNotNull();

		assertThat(replyTo.get()).isNotNull();
		assertThat(replyTo.get()).isEqualTo(replyAddress);
		assertThat(correlationId.get()).isNotNull();
		assertThat(reply.getMessageProperties().getReplyTo()).isNull();
		if (standardHeader) {
			assertThat(reply.getMessageProperties().getCorrelationId()).isNull();
		}
		else {
			assertThat(reply.getMessageProperties().getHeaders().get(CORRELATION_HEADER)).isNull();
		}
	}

	@Test
	public void testReplyToOneDeep() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
		String replyAddress = "new.replyTo";
		template.setReplyAddress(replyAddress);
		template.setReplyTimeout(60000);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setReplyTo("replyTo1");
		messageProperties.setCorrelationId("saveThis");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicReference<String> replyTo = new AtomicReference<String>();
		final AtomicReference<String> correlationId = new AtomicReference<String>();
		willAnswer(invocation -> {
			BasicProperties basicProps = invocation.getArgument(3);
			replyTo.set(basicProps.getReplyTo());
			correlationId.set(basicProps.getCorrelationId());
			MessageProperties springProps = new DefaultMessagePropertiesConverter()
					.toMessageProperties(basicProps, null, "UTF-8");
			Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
			template.onMessage(replyMessage, mock(Channel.class));
			return null;
		}).given(mockChannel).basicPublish(any(String.class), any(String.class), Mockito.anyBoolean(),
				any(BasicProperties.class), any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertThat(reply).isNotNull();

		assertThat(replyTo.get()).isNotNull();
		assertThat(replyTo.get()).isEqualTo(replyAddress);
		assertThat(correlationId.get()).isNotNull();
		assertThat("saveThis".equals(correlationId.get())).isFalse();
		assertThat(reply.getMessageProperties().getReplyTo()).isEqualTo("replyTo1");

	}

	@Test
	public void testReplyToThreeDeep() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		SingleConnectionFactory scf = new SingleConnectionFactory(mockConnectionFactory);
		scf.setExecutor(mock(ExecutorService.class));
		final RabbitTemplate template = new RabbitTemplate(scf);
		String replyTo2 = "replyTo2";
		template.setReplyAddress(replyTo2);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		String replyTo1 = "replyTo1";
		messageProperties.setReplyTo(replyTo1);
		messageProperties.setCorrelationId("a");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicInteger count = new AtomicInteger();
		final List<String> nestedReplyTo = new ArrayList<String>();
		final List<String> nestedCorrelation = new ArrayList<String>();
		final String replyAddress3 = "replyTo3";
		willAnswer(invocation -> {
			BasicProperties basicProps = invocation.getArgument(3);
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
				nestedCorrelation.add(replyMessage.getMessageProperties().getCorrelationId());
			}
			template.onMessage(replyMessage, mock(Channel.class));
			return null;
		}).given(mockChannel).basicPublish(any(String.class), any(String.class), Mockito.anyBoolean(),
				any(BasicProperties.class), any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertThat(reply).isNotNull();

		assertThat(nestedReplyTo).hasSize(3);
		assertThat(nestedReplyTo.get(0)).isEqualTo(replyTo2);
		assertThat(nestedReplyTo.get(1)).isEqualTo(replyAddress3);
		assertThat(nestedReplyTo.get(2)).isEqualTo(replyTo2); // intermediate reply

		assertThat(reply.getMessageProperties().getReplyTo()).isEqualTo(replyTo1);
		assertThat(reply.getMessageProperties().getCorrelationId()).isEqualTo("a");

	}

	@Test
	public void testReplyToOneDeepCustomCorrelationKey() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setCorrelationKey(CORRELATION_HEADER);
		String replyAddress = "new.replyTo";
		template.setReplyAddress(replyAddress);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		String replyTo1 = "replyTo1";
		messageProperties.setReplyTo(replyTo1);
		messageProperties.setCorrelationId("saveThis");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicReference<String> replyTo = new AtomicReference<String>();
		final AtomicReference<String> correlationId = new AtomicReference<String>();
		willAnswer(invocation -> {
			BasicProperties basicProps = invocation.getArgument(3);
			replyTo.set(basicProps.getReplyTo());
			correlationId.set((String) basicProps.getHeaders().get(CORRELATION_HEADER));

			MessageProperties springProps = new DefaultMessagePropertiesConverter()
					.toMessageProperties(basicProps, null, "UTF-8");
			Message replyMessage = new Message("!dlrow olleH".getBytes(), springProps);
			template.onMessage(replyMessage, mock(Channel.class));
			return null;
		}).given(mockChannel).basicPublish(any(String.class), any(String.class), Mockito.anyBoolean(),
				any(BasicProperties.class), any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertThat(reply).isNotNull();

		assertThat(replyTo.get()).isNotNull();
		assertThat(replyTo.get()).isEqualTo(replyAddress);
		assertThat(correlationId.get()).isNotNull();
		assertThat(reply.getMessageProperties().getReplyTo()).isEqualTo(replyTo1);
		assertThat(!"saveThis".equals(correlationId.get())).isTrue();
		assertThat(reply.getMessageProperties().getReplyTo()).isEqualTo(replyTo1);

	}

	@Test
	public void testReplyToThreeDeepCustomCorrelationKey() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setCorrelationKey(CORRELATION_HEADER);
		String replyTo2 = "replyTo2";
		template.setReplyAddress(replyTo2);
		template.expectedQueueNames();

		MessageProperties messageProperties = new MessageProperties();
		String replyTo1 = "replyTo1";
		messageProperties.setReplyTo(replyTo1);
		messageProperties.setCorrelationId("a");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		final AtomicInteger count = new AtomicInteger();
		final List<String> nestedReplyTo = new ArrayList<String>();
		final List<String> nestedCorrelation = new ArrayList<String>();
		final String replyTo3 = "replyTo3";
		willAnswer(invocation -> {
			BasicProperties basicProps = invocation.getArgument(3);
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
			template.onMessage(replyMessage, mock(Channel.class));
			return null;
		}).given(mockChannel).basicPublish(any(String.class), any(String.class), Mockito.anyBoolean(),
				any(BasicProperties.class), any(byte[].class));
		Message reply = template.sendAndReceive(message);
		assertThat(reply).isNotNull();

		assertThat(nestedReplyTo).hasSize(3);
		assertThat(nestedReplyTo.get(0)).isEqualTo(replyTo2);
		assertThat(nestedReplyTo.get(1)).isEqualTo(replyTo3);
		assertThat(nestedReplyTo.get(2)).isEqualTo(replyTo2); //intermediate reply

		assertThat(reply.getMessageProperties().getReplyTo()).isEqualTo(replyTo1);
		assertThat(reply.getMessageProperties().getCorrelationId()).isEqualTo("a");
	}

}
