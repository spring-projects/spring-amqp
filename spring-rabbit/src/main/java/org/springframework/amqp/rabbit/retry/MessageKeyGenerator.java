/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.retry;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
@FunctionalInterface
public interface MessageKeyGenerator {

	/**
	 * Generate a unique key for the message that is repeatable on redelivery. Implementations should be very careful
	 * about assuming uniqueness of any element of the message, especially considering the requirement that it be
	 * repeatable. A message id is ideal but may not be present (AMQP does not mandate it), and the message body is a
	 * byte array whose contents might be repeatable, but its object value is not.
	 * <p>When returning {@code null}, the target service is called immediately and no retry is attempted anymore.
	 * This can be helpful in some scenarios when no more stateful retries are needed for the message,
	 * and it should be rejected after the service call failure.
	 * @param message the message to generate a key for
	 * @return a unique key for this message or null to reject the message if the call fails
	 */
	@Nullable
	Object getKey(Message message);

}
