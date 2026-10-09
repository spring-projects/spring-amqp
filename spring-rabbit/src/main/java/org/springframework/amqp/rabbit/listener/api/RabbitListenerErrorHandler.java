/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.api;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.listener.ListenerExecutionFailedException;

/**
 * An error handler which is called when a {code @RabbitListener} method
 * throws an exception. This is invoked higher up the stack than the
 * listener container's error handler.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0
 *
 */
@FunctionalInterface
public interface RabbitListenerErrorHandler {

	/**
	 * Handle the error. If an exception is not thrown, the return value is returned to
	 * the sender using normal {@code replyTo/@SendTo} semantics.
	 * @param amqpMessage the raw message received.
	 * @param channel AMQP channel for manual acks.
	 * @param message the converted spring-messaging message (if available).
	 * @param exception the exception the listener threw, wrapped in a
	 * {@link ListenerExecutionFailedException}.
	 * @return the return value to be sent to the sender.
	 * @throws Exception an exception which may be the original or different.
	 * @since 3.1.3
	 */
	@Nullable
	Object handleError(Message amqpMessage, @Nullable Channel channel,
			org.springframework.messaging.@Nullable Message<?> message,
			ListenerExecutionFailedException exception) throws Exception;

}
