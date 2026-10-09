/*
 * Copyright 2022-present the original author or authors.
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
