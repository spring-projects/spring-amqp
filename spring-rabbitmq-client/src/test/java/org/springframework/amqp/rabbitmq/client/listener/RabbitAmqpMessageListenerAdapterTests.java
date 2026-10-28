/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client.listener;

import java.time.Duration;

import com.rabbitmq.client.amqp.Consumer;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.config.RabbitAmqpListenerContainerFactory;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
public class RabbitAmqpMessageListenerAdapterTests {

	private static final long NO_TTL = 0xFFFF_FFFFL;

	private final RabbitListenerErrorHandler errorHandler = mock();

	private final Consumer.Context context = mock();

	@Test
	void listenerErrorHandlerIsInvokedForSynchronousFailure() throws Exception {
		RabbitAmqpMessageListenerAdapter adapter = createAdapter(AcknowledgeMode.AUTO);

		adapter.onAmqpMessage(amqpMessage(), null);

		verify(this.errorHandler)
				.handleError(any(), isNull(), any(), any(ListenerExecutionFailedException.class));
	}

	@Test
	void listenerErrorHandlerCanAcknowledgeSynchronousFailure() throws Exception {
		RabbitAmqpMessageListenerAdapter adapter = createAdapter(AcknowledgeMode.MANUAL);

		adapter.onAmqpMessage(amqpMessage(), this.context);

		verify(this.errorHandler)
				.handleError(any(), isNull(), any(), any(ListenerExecutionFailedException.class));
		verify(this.context).accept();
	}

	@Test
	void listenerErrorHandlerExceptionIsPropagated() throws Exception {
		given(this.errorHandler.handleError(any(), any(), any(), any()))
				.willThrow(new IllegalArgumentException("from the error handler"));
		RabbitAmqpMessageListenerAdapter adapter = createAdapter(AcknowledgeMode.MANUAL);

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> adapter.onAmqpMessage(amqpMessage(), this.context))
				.withMessage("from the error handler");
		verify(this.context, never()).accept();
	}

	private RabbitAmqpMessageListenerAdapter createAdapter(AcknowledgeMode ackMode) {
		DefaultMessageHandlerMethodFactory methodFactory = new DefaultMessageHandlerMethodFactory();
		methodFactory.setBeanFactory(new StaticListableBeanFactory());
		methodFactory.afterPropertiesSet();

		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(new FailingBean());
		endpoint.setMethod(ReflectionUtils.findMethod(FailingBean.class, "listen", String.class));
		endpoint.setMessageHandlerMethodFactory(methodFactory);
		endpoint.setQueueNames("test.queue");
		endpoint.setErrorHandler(this.errorHandler);

		AmqpConnectionFactory connectionFactory = mock();
		RabbitAmqpListenerContainerFactory factory = new RabbitAmqpListenerContainerFactory(connectionFactory);
		RabbitAmqpListenerContainer container = factory.createListenerContainer(endpoint);
		RabbitAmqpMessageListenerAdapter adapter = (RabbitAmqpMessageListenerAdapter) container.getMessageListener();
		adapter.containerAckMode(ackMode);
		return adapter;
	}

	private static com.rabbitmq.client.amqp.Message amqpMessage() {
		com.rabbitmq.client.amqp.Message amqpMessage = mock();
		given(amqpMessage.body()).willReturn("test data".getBytes());
		given(amqpMessage.contentType()).willReturn("text/plain");
		given(amqpMessage.ttl()).willReturn(Duration.ofMillis(NO_TTL));
		return amqpMessage;
	}

	static class FailingBean {

		void listen(String data) {
			throw new IllegalStateException("listener failure");
		}

	}

}
