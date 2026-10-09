/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.transaction.interceptor.RuleBasedTransactionAttribute;

/**
 * Subclass of {@link RuleBasedTransactionAttribute} that is aware that
 * listener exceptions are wrapped in {@link ListenerExecutionFailedException}s.
 * Allows users to control rollback based on the actual cause.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6.6
 *
 */
@SuppressWarnings("serial")
public class ListenerFailedRuleBasedTransactionAttribute extends RuleBasedTransactionAttribute {

	@Override
	public boolean rollbackOn(Throwable ex) {
		if (ex instanceof ListenerExecutionFailedException && ex.getCause() != null) {
			return super.rollbackOn(ex.getCause());
		}
		else {
			return super.rollbackOn(ex);
		}
	}

}
