/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpIOException;

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
