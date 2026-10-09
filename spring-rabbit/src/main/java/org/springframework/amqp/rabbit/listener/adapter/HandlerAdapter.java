/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import org.springframework.amqp.listener.adapter.DelegatingInvocableHandler;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * A wrapper for either an {@link InvocableHandlerMethod} or
 * {@link DelegatingInvocableHandler}. All methods delegate to the
 * underlying handler.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.5
 *
 * @deprecated since 4.1 in favor of {@link org.springframework.amqp.listener.adapter.HandlerAdapter}
 */
@Deprecated(since = "4.1", forRemoval = true)
public class HandlerAdapter extends org.springframework.amqp.listener.adapter.HandlerAdapter {

	/**
	 * Construct an instance with the provided method.
	 * @param invokerHandlerMethod the method.
	 */
	public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
		super(invokerHandlerMethod);
	}

	/**
	 * Construct an instance with the provided delegating handler.
	 * @param delegatingHandler the handler.
	 */
	public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
		super(delegatingHandler);
	}

}
