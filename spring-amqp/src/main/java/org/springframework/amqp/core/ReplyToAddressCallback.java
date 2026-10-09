/*
 * Copyright 2013-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * To be used with the receive-and-reply methods of {@link org.springframework.amqp.core.AmqpTemplate}
 * to determine {@link org.springframework.amqp.core.Address} for {@link org.springframework.amqp.core.Message}
 * to send at runtime.
 *
 * <p>This often as an anonymous class within a method implementation.
 *
 * @param <T> the reply type.
 * @author Artem Bilan
 * @author Gary Russell
 * @since 1.3
 */
@FunctionalInterface
public interface ReplyToAddressCallback<T> {

	Address getReplyToAddress(Message request, T reply);

}
