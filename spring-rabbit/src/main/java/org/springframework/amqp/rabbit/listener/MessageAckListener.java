/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.jspecify.annotations.Nullable;

/**
 * A listener for message ack when using {@link org.springframework.amqp.core.AcknowledgeMode#AUTO}.
 *
 * @author Cao Weibo
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.4.6
 */
@FunctionalInterface
public interface MessageAckListener {

	/**
	 * Listener callback.
	 * @param success Whether ack succeed.
	 * @param deliveryTag The deliveryTag of ack.
	 * @param cause The cause of failed ack.
	 */
	void onComplete(boolean success, long deliveryTag, @Nullable Throwable cause);

}
