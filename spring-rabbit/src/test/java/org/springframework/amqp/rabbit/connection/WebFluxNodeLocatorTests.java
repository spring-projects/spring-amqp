/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Ngoc Nhaan
 *
 * @since 4.2
 */
class WebFluxNodeLocatorTests {

	private final WebFluxNodeLocator nodeLocator = new WebFluxNodeLocator();

	@SuppressWarnings("unchecked")
	@Test
	void queueInfoIsRetrievedFromEncodedUri() throws URISyntaxException {

		WebClient webClient = mock();
		WebClient.RequestHeadersUriSpec<?> request = mock();
		WebClient.RequestHeadersSpec<?> headers = mock();
		WebClient.ResponseSpec response = mock();

		willReturn(request).given(webClient).get();
		willReturn(headers).given(request).uri(any(URI.class));
		willReturn(headers).given(headers).accept(MediaType.APPLICATION_JSON);
		willReturn(response).given(headers).retrieve();
		willReturn(Mono.just(Map.of("node", "rabbit@host")))
				.given(response).bodyToMono(any(ParameterizedTypeReference.class));

		Map<String, Object> queueInfo = this.nodeLocator.restCall(webClient,
				"http://localhost:15672/api/queues/", "/", "some queue");

		assertThat(queueInfo).containsEntry("node", "rabbit@host");
	}

	@SuppressWarnings("unchecked")
	@Test
	void apiPathIsResolvedAgainstTheHost() throws URISyntaxException {

		WebClient webClient = mock();
		WebClient.RequestHeadersUriSpec<?> request = mock();
		WebClient.RequestHeadersSpec<?> headers = mock();
		WebClient.ResponseSpec response = mock();

		willReturn(request).given(webClient).get();
		willReturn(headers).given(request).uri(any(URI.class));
		willReturn(headers).given(headers).accept(MediaType.APPLICATION_JSON);
		willReturn(response).given(headers).retrieve();
		willReturn(Mono.just(Map.of("node", "rabbit@host")))
				.given(response).bodyToMono(any(ParameterizedTypeReference.class));

		Map<String, Object> queueInfo = this.nodeLocator.restCall(webClient,
				"http://localhost:15672/api/", "vhost", "queue");

		assertThat(queueInfo).containsEntry("node", "rabbit@host");
	}

}
