/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.Test;

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
		when(channel1.isOpen()).thenReturn(true);
		final Channel channel2 = mock(Channel.class);
		when(channel2.isOpen()).thenReturn(true);
		final com.rabbitmq.client.Connection rabbitConn = mock(AutorecoveringConnection.class);
		when(rabbitConn.isOpen()).thenReturn(true);
		com.rabbitmq.client.ConnectionFactory cf = mock(com.rabbitmq.client.ConnectionFactory.class);
		doAnswer(invocation -> rabbitConn).when(cf).newConnection(any(ExecutorService.class), anyString());
		when(rabbitConn.createChannel()).thenReturn(channel1).thenReturn(channel2);

		CachingConnectionFactory ccf = new CachingConnectionFactory(cf);
		ccf.setExecutor(mock(ExecutorService.class));
		Connection conn1 = ccf.createConnection();
		Channel channel = conn1.createChannel(false);
		verifyChannelIs(channel1, channel);
		channel.close();
		conn1.close();
		Connection conn2 = ccf.createConnection();
		assertSame(conn1, conn2);
		channel = conn1.createChannel(false);
		verifyChannelIs(channel1, channel);
		channel.close();
		conn2.close();

		when(rabbitConn.isOpen()).thenReturn(false).thenReturn(true);
		when(channel1.isOpen()).thenReturn(false);
		conn2 = ccf.createConnection();
		try {
			conn2.createChannel(false);
			fail("Expected AutoRecoverConnectionNotCurrentlyOpenException");
		}
		catch (AutoRecoverConnectionNotCurrentlyOpenException e) {
			assertThat(e.getMessage(), equalTo("Auto recovery connection is not currently open"));
		}
		channel = conn2.createChannel(false);
		verifyChannelIs(channel2, channel);
		channel.close();

		verify(rabbitConn, never()).close();
		verify(channel1).close(); // physically closed to defeat recovery
	}

	private void verifyChannelIs(Channel mockChannel, Channel channel) {
		ChannelProxy proxy = (ChannelProxy) channel;
		assertSame(mockChannel, proxy.getTargetChannel());
	}

}
