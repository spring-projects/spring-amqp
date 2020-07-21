/*
 * Copyright 2014-2020 the original author or authors.
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
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.4
 *
 */
public class ClientRecoveryCompatibilityTests {

	@Test
	public void testDefeatRecovery() throws Exception {
		final Channel channel1 = mock(Channel.class);
		given(channel1.isOpen()).willReturn(true);
		final Channel channel2 = mock(Channel.class);
		given(channel2.isOpen()).willReturn(true);
		final com.rabbitmq.client.Connection rabbitConn = mock(AutorecoveringConnection.class);
		given(rabbitConn.isOpen()).willReturn(true);
		com.rabbitmq.client.ConnectionFactory cf = mock(com.rabbitmq.client.ConnectionFactory.class);
		willAnswer(invocation -> rabbitConn).given(cf).newConnection(any(ExecutorService.class), anyString());
		given(rabbitConn.createChannel()).willReturn(channel1).willReturn(channel2);

		CachingConnectionFactory ccf = new CachingConnectionFactory(cf);
		ccf.setExecutor(mock(ExecutorService.class));
		Connection conn1 = ccf.createConnection();
		Channel channel = conn1.createChannel(false);
		verifyChannelIs(channel1, channel);
		channel.close();
		conn1.close();
		Connection conn2 = ccf.createConnection();
		assertThat(conn2).isSameAs(conn1);
		channel = conn1.createChannel(false);
		verifyChannelIs(channel1, channel);
		channel.close();
		conn2.close();

		given(rabbitConn.isOpen()).willReturn(false).willReturn(true);
		given(channel1.isOpen()).willReturn(false);
		conn2 = ccf.createConnection();
		try {
			conn2.createChannel(false);
			fail("Expected AutoRecoverConnectionNotCurrentlyOpenException");
		}
		catch (AutoRecoverConnectionNotCurrentlyOpenException e) {
			assertThat(e.getMessage()).isEqualTo("Auto recovery connection is not currently open");
		}
		channel = conn2.createChannel(false);
		verifyChannelIs(channel2, channel);
		channel.close();

		verify(rabbitConn, never()).close();
		verify(channel1).close(); // physically closed to defeat recovery
	}

	private void verifyChannelIs(Channel mockChannel, Channel channel) {
		ChannelProxy proxy = (ChannelProxy) channel;
		assertThat(proxy.getTargetChannel()).isSameAs(mockChannel);
	}

}
