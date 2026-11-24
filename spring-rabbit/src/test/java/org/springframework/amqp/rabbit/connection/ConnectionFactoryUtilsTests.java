/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.connection;

import org.junit.jupiter.api.Test;

import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 1.7.1
 *
 */
public class ConnectionFactoryUtilsTests {

	@Test
	public void testResourceHolder() {
		RabbitResourceHolder h1 = new RabbitResourceHolder();
		RabbitResourceHolder h2 = new RabbitResourceHolder();
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		TransactionSynchronizationManager.setActualTransactionActive(true);
		ConnectionFactoryUtils.bindResourceToTransaction(h1, connectionFactory, true);
		assertThat(ConnectionFactoryUtils.bindResourceToTransaction(h2, connectionFactory, true)).isSameAs(h1);
		TransactionSynchronizationManager.clear();
	}

}
