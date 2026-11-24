/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class LocallyTransactedSMLCTests extends LocallyTransactedTests {

	@Override
	protected AbstractMessageListenerContainer createContainer(AbstractConnectionFactory connectionFactory) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setReceiveTimeout(10);
		return container;
	}

}
