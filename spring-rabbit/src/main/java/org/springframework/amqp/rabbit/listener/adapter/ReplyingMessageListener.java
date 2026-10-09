/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

/**
 * A Message Listener that returns a reply - intended for lambda use in a
 * {@link MessageListenerAdapter}.
 * @param <T> the request type.
 * @param <R> the reply type.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface ReplyingMessageListener<T, R> {

	/**
	 * Handle the message and return a reply.
	 * @param t the request.
	 * @return the reply.
	 */
	R handleMessage(T t);

}
