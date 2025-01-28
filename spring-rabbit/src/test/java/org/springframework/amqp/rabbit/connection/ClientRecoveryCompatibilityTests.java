/*
 * Copyright 2014-2025 the original author or authors.
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

import java.util.concurrent.ExecutorService;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.4
 *
 */
public class ClientRecoveryCompatibilityTests {

	@Test
	public void testDefeatRecovery() throws Exception {
		RabbitUtils.clearPhysicalCloseRequired(); // left over from some other test
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
		ChannelProxy proxy = (ChannelProxy) channel;
		assertThat(proxy.getTargetChannel()).isSameAs(channel1);
		channel.close();
		conn1.close();
		Connection conn2 = ccf.createConnection();
		assertThat(conn2).isSameAs(conn1);
		channel = conn1.createChannel(false);
		proxy = (ChannelProxy) channel;
		assertThat(proxy.getTargetChannel()).isSameAs(channel1);
		channel.close();
		conn2.close();

		given(rabbitConn.isOpen()).willReturn(false).willReturn(true);
		given(channel1.isOpen()).willReturn(false);
		Connection conn3 = ccf.createConnection();
		assertThat(conn3).isSameAs(conn1);
		assertThatExceptionOfType(AutoRecoverConnectionNotCurrentlyOpenException.class).isThrownBy(() ->
					conn3.createChannel(false))
				.withMessage("Auto recovery connection is not currently open");
		channel = conn2.createChannel(false);
		proxy = (ChannelProxy) channel;
		assertThat(proxy.getTargetChannel()).isSameAs(channel2);
		channel.close();

		verify(rabbitConn, never()).close();
		verify(channel1).close(); // physically closed to defeat recovery
	}

}
