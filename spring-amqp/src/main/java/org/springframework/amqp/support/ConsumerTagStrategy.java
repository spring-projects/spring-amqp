/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp.support;

/**
 * A strategy interface to determine the consumer tag to be used when issuing a
 * {@code basicConsume} operation.
 *
 * @author Gary Russell
 * @since 1.4.5
 *
 */
@FunctionalInterface
public interface ConsumerTagStrategy {

	/**
	 * Create the consumer tag, optionally based on the queue name that the consumer
	 * will listen to. Consumer tags must be unique.
	 * @param queue The queue name that this consumer will listen to.
	 * @return The consumer tag.
	 */
	String createConsumerTag(String queue);

}
