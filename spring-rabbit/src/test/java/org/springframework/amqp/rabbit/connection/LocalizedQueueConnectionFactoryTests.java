/*
 * Copyright 2015-2022 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.test.web.reactive.server.HttpHandlerConnector;
import org.springframework.web.reactive.function.client.WebClient;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import reactor.core.publisher.Mono;


/**
 * @author Gary Russell
 * @author Artem Bilan
 */
public class LocalizedQueueConnectionFactoryTests {

	private final Map<String, Channel> channels = new HashMap<String, Channel>();

	private final Map<String, Consumer> consumers = new HashMap<String, Consumer>();

	private final Map<String, String> consumerTags = new HashMap<String, String>();

	@Test
	public void testFailOver() throws Exception {
		ConnectionFactory defaultConnectionFactory = mockCF("localhost:1234", null);
		String rabbit1 = "localhost:1235";
		String rabbit2 = "localhost:1236";
		String[] addresses = new String[]{rabbit1, rabbit2};
		String[] adminUris = new String[]{"http://localhost:11235", "http://localhost:11236"};
		String[] nodes = new String[]{"rabbit@foo", "rabbit@bar"};
		String vhost = "/";
		String username = "guest";
		String password = "guest";
		final AtomicBoolean firstServer = new AtomicBoolean(true);
		final WebClient client1 = doCreateClient(adminUris[0], username, password, nodes[0]);
		final WebClient client2 = doCreateClient(adminUris[1], username, password, nodes[1]);
		final Map<String, ConnectionFactory> mockCFs = new HashMap<String, ConnectionFactory>();
		CountDownLatch latch1 = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(1);
		mockCFs.put(rabbit1, mockCF(rabbit1, latch1));
		mockCFs.put(rabbit2, mockCF(rabbit2, latch2));
		LocalizedQueueConnectionFactory lqcf = new LocalizedQueueConnectionFactory(defaultConnectionFactory, addresses,
				adminUris, nodes, vhost, username, password, false, null) {

			@Override
			protected ConnectionFactory createConnectionFactory(String address, String node) {
				return mockCFs.get(address);
			}

		};
		lqcf.setNodeLocator(new WebFluxNodeLocator() {

			@Override
			public WebClient createClient(String username, String password) {
				return firstServer.get() ? client1 : client2;
			}

		});
		Map<?, ?> nodeAddress = TestUtils.getPropertyValue(lqcf, "nodeToAddress", Map.class);
		assertThat(nodeAddress.get("rabbit@foo")).isEqualTo(rabbit1);
		assertThat(nodeAddress.get("rabbit@bar")).isEqualTo(rabbit2);
		String[] admins = TestUtils.getPropertyValue(lqcf, "adminUris", String[].class);
		assertThat(admins).containsExactly(adminUris);
		Log logger = spy(TestUtils.getPropertyValue(lqcf, "logger", Log.class));
		willReturn(true).given(logger).isInfoEnabled();
		new DirectFieldAccessor(lqcf).setPropertyValue("logger", logger);
		willAnswer(new CallsRealMethods()).given(logger).debug(anyString());
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(lqcf);
		container.setQueueNames("q");
		container.afterPropertiesSet();
		container.start();
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		Channel channel = this.channels.get(rabbit1);
		assertThat(channel).isNotNull();
		verify(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(),
				anyBoolean(), anyMap(),
				any(Consumer.class));
		verify(logger, atLeast(1)).info(captor.capture());
		assertThat(assertLog(captor.getAllValues(), "Queue: q is on node: rabbit@foo at: localhost:1235")).isTrue();

		// Fail rabbit1 and verify the container switches to rabbit2

		firstServer.set(false);
		this.consumers.get(rabbit1).handleCancel(consumerTags.get(rabbit1));
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		channel = this.channels.get(rabbit2);
		assertThat(channel).isNotNull();
		verify(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(),
				anyBoolean(), anyMap(),
				any(Consumer.class));
		container.stop();
		verify(logger, atLeast(1)).info(captor.capture());
		assertThat(assertLog(captor.getAllValues(), "Queue: q is on node: rabbit@bar at: localhost:1236")).isTrue();
	}

	private boolean assertLog(List<String> logRows, String expected) {
		for (String log : logRows) {
			if (log.contains(expected)) {
				return true;
			}
		}
		return false;
	}

	private WebClient doCreateClient(String uri, String username, String password, String node) {
		ClientHttpConnector httpConnector =
				new HttpHandlerConnector((request, response) -> {
					response.setStatusCode(HttpStatus.OK);
					response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
					Mono<DataBuffer> json = Mono
							.just(response.bufferFactory().wrap(("{\"node\":\"" + node + "\"}").getBytes()));
					return response.writeWith(json)
							.then(Mono.defer(response::setComplete));
				});

		return WebClient.builder()
				.clientConnector(httpConnector)
				.build();
	}

	@Test
	public void test2Queues() throws Exception {
		try {
			String rabbit1 = "localhost:1235";
			String rabbit2 = "localhost:1236";
			String[] addresses = new String[]{rabbit1, rabbit2};
			String[] adminUris = new String[]{"http://localhost:11235", "http://localhost:11236"};
			String[] nodes = new String[]{"rabbit@foo", "rabbit@bar"};
			String vhost = "/";
			String username = "guest";
			String password = "guest";
			LocalizedQueueConnectionFactory lqcf = new LocalizedQueueConnectionFactory(mockCF("localhost:1234", null),
					addresses, adminUris, nodes, vhost, username, password, false, null);
			lqcf.getTargetConnectionFactory("[foo, bar]");
		}
		catch (IllegalArgumentException e) {
			assertThat(e.getMessage()).contains("Cannot use LocalizedQueueConnectionFactory with more than one queue: [foo, bar]");
		}
	}

	private ConnectionFactory mockCF(final String address, final CountDownLatch latch) throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		given(connection.isOpen()).willReturn(true, false);
		given(channel.isOpen()).willReturn(true, false);
		willAnswer(invocation -> {
			String tag = UUID.randomUUID().toString();
			this.consumers.put(address, invocation.getArgument(6));
			this.consumerTags.put(address, tag);
			if (latch != null) {
				latch.countDown();
			}
			return tag;
		}).given(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
				any(Consumer.class));
		given(connectionFactory.getHost()).willReturn(address);
		this.channels.put(address, channel);
		return connectionFactory;
	}

}
