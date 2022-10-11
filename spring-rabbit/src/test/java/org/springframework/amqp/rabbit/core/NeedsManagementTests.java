/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;

import org.springframework.amqp.rabbit.junit.BrokerRunningSupport;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;

/**
 * @author Gary Russell
 * @since 2.4.8
 *
 */
public abstract class NeedsManagementTests {

	protected static BrokerRunningSupport brokerRunning;

	@BeforeAll
	static void setUp() {
		brokerRunning = RabbitAvailableCondition.getBrokerRunning();
	}

	protected Map<String, Object> queueInfo(String queueName) throws URISyntaxException {
		WebClient client = createClient(brokerRunning.getAdminUser(), brokerRunning.getAdminPassword());
		URI uri = queueUri(queueName);
		return client.get()
				.uri(uri)
				.accept(MediaType.APPLICATION_JSON)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	protected Map<String, Object> exchangeInfo(String name) throws URISyntaxException {
		WebClient client = createClient(brokerRunning.getAdminUser(), brokerRunning.getAdminPassword());
		URI uri = exchangeUri(name);
		return client.get()
				.uri(uri)
				.accept(MediaType.APPLICATION_JSON)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
				})
				.block(Duration.ofSeconds(10));
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> arguments(Map<String, Object> infoMap) {
		return (Map<String, Object>) infoMap.get("arguments");
	}

	private URI queueUri(String queue) throws URISyntaxException {
		URI uri = new URI(brokerRunning.getAdminUri())
				.resolve("/api/queues/" + UriUtils.encodePathSegment("/", StandardCharsets.UTF_8) + "/" + queue);
		return uri;
	}

	private URI exchangeUri(String queue) throws URISyntaxException {
		URI uri = new URI(brokerRunning.getAdminUri())
				.resolve("/api/exchanges/" + UriUtils.encodePathSegment("/", StandardCharsets.UTF_8) + "/" + queue);
		return uri;
	}

	private WebClient createClient(String adminUser, String adminPassword) {
		return WebClient.builder()
				.filter(ExchangeFilterFunctions.basicAuthentication(adminUser, adminPassword))
				.build();
	}

}
