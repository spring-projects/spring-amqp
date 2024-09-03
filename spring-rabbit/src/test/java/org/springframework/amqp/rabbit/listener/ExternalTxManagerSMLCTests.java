/*
 * Copyright 2016-2024 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationEventPublisher;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

/**
 * @author Gary Russell
 * @author Thomas Badie
 * @since 2.0
 *
 */
public class ExternalTxManagerSMLCTests extends ExternalTxManagerTests {

	@Override
	protected AbstractMessageListenerContainer createContainer(AbstractConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setReceiveTimeout(60000);
		return container;
	}


	@Test
	public void testMessageListenerTxFail() throws Exception {
		ConnectionFactoryUtils.enableAfterCompletionFailureCapture(true);
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockChannel.txSelect()).willReturn(mock(AMQP.Tx.SelectOk.class));
		final AtomicReference<CountDownLatch> commitLatch = new AtomicReference<>(new CountDownLatch(1));
		String exceptionMessage = "Failed to commit.";
		willAnswer(invocation -> {
			commitLatch.get().countDown();
			throw new IllegalStateException(exceptionMessage);
		}).given(mockChannel).txCommit();

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);
		cachingConnectionFactory.setExecutor(mock(ExecutorService.class));
		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		willAnswer(invocation -> mockChannel).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(mockChannel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));


		final CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(cachingConnectionFactory);
		container.setMessageListener(message -> {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(cachingConnectionFactory);
			rabbitTemplate.setChannelTransacted(true);
			// should use same channel as container
			rabbitTemplate.convertAndSend("foo", "bar", "baz");
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		DummyTxManager transactionManager = new DummyTxManager();
		container.setTransactionManager(transactionManager);
		ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
		final CountDownLatch applicationEventPublisherLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			if (invocation.getArgument(0) instanceof ListenerContainerConsumerFailedEvent) {
				applicationEventPublisherLatch.countDown();
			}
			return null;
		}).given(applicationEventPublisher).publishEvent(any());

		container.setApplicationEventPublisher(applicationEventPublisher);
		container.afterPropertiesSet();
		container.start();
		assertThat(consumerLatch.await(10, TimeUnit.SECONDS)).isTrue();

		consumer.get().handleDelivery("qux",
				new Envelope(1, false, "foo", "bar"), new AMQP.BasicProperties(),
				new byte[] { 0 });

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		verify(mockConnection, times(1)).createChannel();
		assertThat(commitLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
		verify(mockChannel).basicAck(anyLong(), anyBoolean());
		verify(mockChannel).txCommit();

		assertThat(applicationEventPublisherLatch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(applicationEventPublisher).publishEvent(any(ListenerContainerConsumerFailedEvent.class));

		ArgumentCaptor<ListenerContainerConsumerFailedEvent> argumentCaptor
				= ArgumentCaptor.forClass(ListenerContainerConsumerFailedEvent.class);
		verify(applicationEventPublisher).publishEvent(argumentCaptor.capture());
		assertThat(argumentCaptor.getValue().getThrowable()).hasCauseInstanceOf(IllegalStateException.class);
		assertThat(argumentCaptor.getValue().getThrowable())
				.isNotNull().extracting(Throwable::getCause)
				.isNotNull().extracting(Throwable::getMessage).isEqualTo(exceptionMessage);
		container.stop();
	}


}
