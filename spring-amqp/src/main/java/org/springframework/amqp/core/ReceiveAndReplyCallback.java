/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.core;

import org.jspecify.annotations.Nullable;

/**
 * To be used with the receive-and-reply methods of {@link org.springframework.amqp.core.AmqpTemplate}
 * as processor for inbound object and producer for outbound object.
 *
 * <p>This often as an anonymous class within a method implementation.
 * @param <R> The type of the request after conversion from the {@link Message}.
 * @param <S> The type of the response.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.3
 */
@FunctionalInterface
public interface ReceiveAndReplyCallback<R, S> {

	@Nullable
	S handle(R payload);

}
