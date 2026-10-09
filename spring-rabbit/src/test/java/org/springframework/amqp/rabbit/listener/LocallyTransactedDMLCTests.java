/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class LocallyTransactedDMLCTests extends LocallyTransactedTests {

	@Override
	protected AbstractMessageListenerContainer createContainer(AbstractConnectionFactory connectionFactory) {
		return new DirectMessageListenerContainer(connectionFactory);
	}

}
