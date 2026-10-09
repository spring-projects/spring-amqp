/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ThreadChannelConnectionFactory} publisher connection factory
 * configuration. These require no broker.
 *
 * @author Kumar Gaurav
 *
 * @since 4.0.6
 */
class ThreadChannelConnectionFactoryPublisherTests {

	@Test
	void simplePublisherConfirmsPropagateToDefaultPublisherFactory() {
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(new ConnectionFactory());

		tccf.setSimplePublisherConfirms(true);

		assertThat(tccf.isSimplePublisherConfirms()).isTrue();
		org.springframework.amqp.rabbit.connection.ConnectionFactory publisher =
				tccf.getPublisherConnectionFactory();
		assertThat(publisher).isNotNull();
		assertThat(((ThreadChannelConnectionFactory) publisher).isSimplePublisherConfirms())
				.as("simplePublisherConfirms must reach the default publisher sub-factory")
				.isTrue();
	}

}
