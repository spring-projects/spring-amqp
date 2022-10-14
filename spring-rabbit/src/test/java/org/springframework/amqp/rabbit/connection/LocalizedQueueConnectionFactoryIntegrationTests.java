/*
 * Copyright 2015-2019 the original author or authors.
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
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunningSupport;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
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
public class LocalizedQueueConnectionFactoryIntegrationTests {

	private LocalizedQueueConnectionFactory lqcf;

	private CachingConnectionFactory defaultConnectionFactory;

	private RabbitAdmin defaultAdmin;

	@BeforeEach
	public void setup() {
		this.defaultConnectionFactory = new CachingConnectionFactory("localhost");
		this.defaultAdmin = new RabbitAdmin(this.defaultConnectionFactory);
		BrokerRunningSupport brokerRunning = RabbitAvailableCondition.getBrokerRunning();
		String[] addresses = new String[] { "localhost:9999",
				brokerRunning.getHostName() + ":" + brokerRunning.getPort() };
		String[] adminUris = new String[] { brokerRunning.getAdminUri(), brokerRunning.getAdminUri() };
		String[] nodes = new String[] { "foo@bar", findLocalNode() };
		String vhost = "/";
		String username = brokerRunning.getAdminUser();
		String password = brokerRunning.getAdminPassword();
		this.lqcf = new LocalizedQueueConnectionFactory(defaultConnectionFactory, addresses,
				adminUris, nodes, vhost, username, password, false, null);
	}

	@AfterEach
	public void tearDown() {
		if (this.lqcf != null) {
			this.lqcf.destroy();
		}
		if (this.defaultConnectionFactory != null) {
			this.defaultConnectionFactory.destroy();
		}
	}

	@Test
	public void testConnect() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(this.lqcf);
		Queue queue = new Queue(UUID.randomUUID().toString(), false, false, true);
		admin.declareQueue(queue);
		ConnectionFactory targetConnectionFactory = this.lqcf.getTargetConnectionFactory("[" + queue.getName() + "]");
		assertThat(targetConnectionFactory).isNotSameAs(this.defaultConnectionFactory);
		RabbitTemplate template = new RabbitTemplate(targetConnectionFactory);
		template.convertAndSend("", queue.getName(), "foo");
		assertThat(template.receiveAndConvert(queue.getName())).isEqualTo("foo");
		admin.deleteQueue(queue.getName());
	}

	@Test
	void findLocal() {
		ConnectionFactory defaultCf = mock(ConnectionFactory.class);
		BrokerRunningSupport brokerRunning = RabbitAvailableCondition.getBrokerRunning();
		LocalizedQueueConnectionFactory lqcf = new LocalizedQueueConnectionFactory(defaultCf,
				Map.of(findLocalNode(), brokerRunning.getHostName() + ":" + brokerRunning.getPort()),
				new String[] { brokerRunning.getAdminUri() },
				"/", brokerRunning.getAdminUser(), brokerRunning.getAdminPassword(), false, null);
		ConnectionFactory cf = lqcf.getTargetConnectionFactory("[local]");
		RabbitAdmin admin = new RabbitAdmin(cf);
		assertThat(admin.getQueueProperties("local")).isNotNull();
		lqcf.setNodeLocator(new RestTemplateNodeLocator());
		ConnectionFactory cf2 = lqcf.getTargetConnectionFactory("[local]");
		assertThat(cf2).isSameAs(cf);
		lqcf.destroy();
	}

	private String findLocalNode() {
		AnonymousQueue queue = new AnonymousQueue();
		this.defaultAdmin.declareQueue(queue);
		URI uri;
		BrokerRunningSupport brokerRunning = RabbitAvailableCondition.getBrokerRunning();
		try {
			uri = new URI(brokerRunning.getAdminUri())
					.resolve("/api/queues/" + UriUtils.encodePathSegment("/", StandardCharsets.UTF_8) + "/"
							+ queue.getName());
		}
		catch (URISyntaxException ex) {
			throw new IllegalStateException(ex);
		}
		WebClient client = WebClient.builder()
				.filter(ExchangeFilterFunctions.basicAuthentication(brokerRunning.getAdminUser(),
						brokerRunning.getAdminPassword()))
				.build();
		Map<String, Object> queueInfo = client.get()
				.uri(uri)
				.accept(MediaType.APPLICATION_JSON)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
				})
				.block(Duration.ofSeconds(10));
		this.defaultAdmin.deleteQueue(queue.getName());
		return (String) queueInfo.get("node");
	}

}
