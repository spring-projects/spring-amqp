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
import static org.mockito.Mockito.mock;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;


/**
 *
 * @author Gary Russell
 */
@RabbitAvailable(management = true, queues = "local")
public class LocalizedQueueConnectionFactoryIntegrationTests extends AbstractTestContainerTests {

	private LocalizedQueueConnectionFactory lqcf;

	private CachingConnectionFactory defaultConnectionFactory;

	private CachingConnectionFactory testContainerFactory;

	private RabbitAdmin defaultAdmin;

	private RabbitAdmin testContainerAdmin;

	@BeforeEach
	public void setup() {
		this.defaultConnectionFactory = new CachingConnectionFactory("localhost");
		this.defaultAdmin = new RabbitAdmin(this.defaultConnectionFactory);
		this.testContainerFactory = new CachingConnectionFactory("localhost", amqpPort());
		this.testContainerAdmin = new RabbitAdmin(this.testContainerFactory);
		String[] addresses = new String[] { "localhost:5672", "localhost:" + amqpPort() };
		String[] adminUris = new String[] { "http://localhost:15672", "http://localhost:" + managementPort() };
		String[] nodes = new String[] { "rabbit@localhost", findTcNode() };
		String vhost = "/";
		String username = "guest";
		String password = "guest";
		this.lqcf = new LocalizedQueueConnectionFactory(defaultConnectionFactory, addresses,
				adminUris, nodes, vhost, username, password, false, null);
	}

	@AfterEach
	public void tearDown() {
		this.lqcf.destroy();
		this.defaultConnectionFactory.destroy();
		this.testContainerFactory.destroy();
	}

	@Test
	public void testFindCorrectConnection() throws Exception {
		AnonymousQueue externalQueue = new AnonymousQueue();
		AnonymousQueue tcQueue = new AnonymousQueue();
		this.defaultAdmin.declareQueue(externalQueue);
		this.testContainerAdmin.declareQueue(tcQueue);
		ConnectionFactory cf = this.lqcf
				.getTargetConnectionFactory("[" + externalQueue.getName() + "]");
		assertThat(cf).isNotSameAs(this.defaultConnectionFactory);
		assertThat(this.defaultAdmin.getQueueProperties(externalQueue.getName())).isNotNull();
		cf = this.lqcf.getTargetConnectionFactory("[" + tcQueue.getName() + "]");
		assertThat(cf).isNotSameAs(this.defaultConnectionFactory);
		assertThat(this.testContainerAdmin.getQueueProperties(tcQueue.getName())).isNotNull();
		this.defaultAdmin.deleteQueue(externalQueue.getName());
		this.testContainerAdmin.deleteQueue(tcQueue.getName());
	}

	@Test
	void findLocal() {
		ConnectionFactory defaultCf = mock(ConnectionFactory.class);
		LocalizedQueueConnectionFactory lqcf = new LocalizedQueueConnectionFactory(defaultCf,
				Map.of("rabbit@localhost", "localhost:5672"), new String[] { "http://localhost:15672" },
				"/", "guest", "guest", false, null);
		ConnectionFactory cf = lqcf.getTargetConnectionFactory("[local]");
		RabbitAdmin admin = new RabbitAdmin(cf);
		assertThat(admin.getQueueProperties("local")).isNotNull();
		lqcf.setNodeLocator(new RestTemplateNodeLocator());
		ConnectionFactory cf2 = lqcf.getTargetConnectionFactory("[local]");
		assertThat(cf2).isSameAs(cf);
		lqcf.destroy();
	}

	private String findTcNode() {
		AnonymousQueue queue = new AnonymousQueue();
		this.testContainerAdmin.declareQueue(queue);
		URI uri;
		try {
			uri = new URI(restUri())
					.resolve("/api/queues/" + UriUtils.encodePathSegment("/", StandardCharsets.UTF_8) + "/"
							+ queue.getName());
		}
		catch (URISyntaxException ex) {
			throw new IllegalStateException(ex);
		}
		WebClient client = WebClient.builder()
				.filter(ExchangeFilterFunctions.basicAuthentication(RABBITMQ.getAdminUsername(),
						RABBITMQ.getAdminPassword()))
				.build();
		Map<String, Object> queueInfo = client.get()
				.uri(uri)
				.accept(MediaType.APPLICATION_JSON)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
				})
				.block(Duration.ofSeconds(10));
		this.testContainerAdmin.deleteQueue(queue.getName());
		return (String) queueInfo.get("node");
	}


}
