/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Norbert Wirges
 * @since 1.7.1
 *
 */
public class ConnectionFactoryUtilsTests {

	@AfterEach
	void tearDown() {
		ConsumerChannelRegistry.unRegisterConsumerChannel();
		TransactionSynchronizationManager.clear();
	}

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

	@Test
	void consumerChannelIsUsedByOneTransactionOnly() {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Channel consumerChannel = mock(Channel.class);
		Channel otherChannel = mockConnectionWithChannel(connectionFactory);
		ConsumerChannelRegistry.registerConsumerChannel(consumerChannel, connectionFactory);

		// the container's transaction
		assertThat(ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, true).getChannel())
				.isSameAs(consumerChannel);
		// a REQUIRES_NEW transaction while the container's transaction is suspended
		assertThat(ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, true).getChannel())
				.isSameAs(otherChannel);
		assertThat(ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, true, true).getChannel())
				.isSameAs(otherChannel);

		// the next container transaction can use the consumer channel again
		ConsumerChannelRegistry.releaseConsumerChannelClaim();
		assertThat(ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, true).getChannel())
				.isSameAs(consumerChannel);
	}

	@Test
	void consumerChannelBoundToTransactionIsNotUsedByNewTransaction() {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Channel consumerChannel = mock(Channel.class);
		Channel otherChannel = mockConnectionWithChannel(connectionFactory);
		ConsumerChannelRegistry.registerConsumerChannel(consumerChannel, connectionFactory);

		// the container binds the consumer channel to its transaction
		TransactionSynchronizationManager.setActualTransactionActive(true);
		ConnectionFactoryUtils.bindResourceToTransaction(new RabbitResourceHolder(consumerChannel, false),
				connectionFactory, true);
		assertThat(ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, true, false).getChannel())
				.isSameAs(consumerChannel);

		// the container's transaction is suspended by a REQUIRES_NEW transaction
		TransactionSynchronizationManager.unbindResource(connectionFactory);
		assertThat(ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, true, false).getChannel())
				.isSameAs(otherChannel);
	}

	@Test
	void releaseResourcesDoesNotCloseConsumerChannel() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Channel consumerChannel = mock(Channel.class);
		ConsumerChannelRegistry.registerConsumerChannel(consumerChannel, connectionFactory);

		ConnectionFactoryUtils.releaseResources(new RabbitResourceHolder(consumerChannel, true));
		verify(consumerChannel, never()).close();

		ConsumerChannelRegistry.unRegisterConsumerChannel();
		ConnectionFactoryUtils.releaseResources(new RabbitResourceHolder(consumerChannel, true));
		verify(consumerChannel).close();
	}

	private static Channel mockConnectionWithChannel(ConnectionFactory connectionFactory) {
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		return channel;
	}

}
