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

package org.springframework.amqp.rabbit.core;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.util.ErrorHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0
 *
 */
public class RabbitTemplateDirectReplyToContainerIntegrationTests extends RabbitTemplateIntegrationTests {

	@Override
	protected RabbitTemplate createSendAndReceiveRabbitTemplate(ConnectionFactory connectionFactory) {
		RabbitTemplate rabbitTemplate = super.createSendAndReceiveRabbitTemplate(connectionFactory);
		rabbitTemplate.setUseDirectReplyToContainer(true);
		rabbitTemplate.setBeanName(this.testName + ".SendReceiveRabbitTemplate");
		return rabbitTemplate;
	}

	@Test
	public void channelReleasedOnTimeout() throws Exception {
		final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		RabbitTemplate rabbitTemplate = createSendAndReceiveRabbitTemplate(connectionFactory);
		rabbitTemplate.setReplyTimeout(1);
		AtomicReference<Throwable> exception = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		ErrorHandler replyErrorHandler = t -> {
			exception.set(t);
			latch.countDown();
		};
		rabbitTemplate.setReplyErrorHandler(replyErrorHandler);
		Object reply = rabbitTemplate.convertSendAndReceive(ROUTE, "foo");
		assertThat(reply).isNull();
		Object container = TestUtils.<Map<?, ?>>getPropertyValue(rabbitTemplate, "directReplyToContainers")
				.get(rabbitTemplate.isUsePublisherConnection()
						? connectionFactory.getPublisherConnectionFactory()
						: connectionFactory);
		assertThat(TestUtils.<Map<?, ?>>getPropertyValue(container, "inUseConsumerChannels")).isEmpty();
		assertThat(TestUtils.<ErrorHandler>getPropertyValue(container, "errorHandler")).isSameAs(replyErrorHandler);
		Message replyMessage = new Message("foo".getBytes(), new MessageProperties());
		assertThatThrownBy(() -> rabbitTemplate.onMessage(replyMessage, mock(Channel.class)))
				.isInstanceOf(AmqpRejectAndDontRequeueException.class)
				.hasMessage("No correlation header in reply");
		replyMessage.getMessageProperties().setCorrelationId("foo");
		assertThatThrownBy(() -> rabbitTemplate.onMessage(replyMessage, mock(Channel.class)))
				.isInstanceOf(AmqpRejectAndDontRequeueException.class)
				.hasMessage("Reply received after timeout");

		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		executor.submit(() -> {
			Message message = rabbitTemplate.receive(ROUTE, 10_000);
			assertThat(message).as("No message received").isNotNull();
			rabbitTemplate.send(message.getMessageProperties().getReplyTo(), replyMessage);
			return message;
		});
		while (rabbitTemplate.receive(ROUTE, 100) != null) {
			// empty
		}
		reply = rabbitTemplate.convertSendAndReceive(ROUTE, "foo");
		assertThat(reply).isNull();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(exception.get()).isInstanceOf(ListenerExecutionFailedException.class);
		assertThat(exception.get().getCause().getMessage()).isEqualTo("Reply received after timeout");
		assertThat(((ListenerExecutionFailedException) exception.get()).getFailedMessage().getBody())
				.isEqualTo(replyMessage.getBody());
		assertThat(TestUtils.<Map<?, ?>>getPropertyValue(container, "inUseConsumerChannels")).isEmpty();
		executor.shutdownNow();
		rabbitTemplate.stop();
		connectionFactory.destroy();
	}

}
