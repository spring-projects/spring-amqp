/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpIOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Gary Russell
 * @author DongMin Park
 * @since 2.2.17
 *
 */
public class ConnectionListenerTests {

	@Test
	void cantConnectCCF() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(rcf());
		cantConnect(ccf);
	}

	@Test
	void cantConnectTCCF() {
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rcf());
		cantConnect(tccf);
	}

	@Test
	void cantConnectPCCF() {
		PooledChannelConnectionFactory pccf = new PooledChannelConnectionFactory(rcf());
		cantConnect(pccf);
	}

	private com.rabbitmq.client.ConnectionFactory rcf() {
		com.rabbitmq.client.ConnectionFactory rcf = new com.rabbitmq.client.ConnectionFactory();
		rcf.setHost("junk.host");
		return rcf;
	}

	private void cantConnect(ConnectionFactory cf) {
		AtomicBoolean failed = new AtomicBoolean();
		cf.addConnectionListener(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
			}

			@Override
			public void onFailed(Exception exception) {
				failed.set(true);
			}

		});
		assertThatExceptionOfType(AmqpIOException.class).isThrownBy(() -> cf.createConnection());
		assertThat(failed.get()).isTrue();
	}

}
