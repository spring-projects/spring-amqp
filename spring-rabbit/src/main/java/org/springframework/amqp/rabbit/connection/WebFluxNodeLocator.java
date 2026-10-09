/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;

/**
 * A {@link NodeLocator} using the Spring WebFlux {@link WebClient}.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 2.4.8
 *
 */
public class WebFluxNodeLocator implements NodeLocator<WebClient> {

	@Override
	public @Nullable Map<String, Object> restCall(WebClient client, String baseUri, String vhost, String queue)
			throws URISyntaxException {

		URI uri = new URI(baseUri)
				.resolve("/api/queues/"
						+ UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/"
						+ UriUtils.encodePathSegment(queue, StandardCharsets.UTF_8));
		return client.get()
				.uri(uri)
				.accept(MediaType.APPLICATION_JSON)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<HashMap<String, Object>>() {

				})
				.block(Duration.ofSeconds(10));
	}

	/**
	 * Create a client instance.
	 * @param username the username
	 * @param password the password.
	 * @return The client.
	 */
	@Override
	public WebClient createClient(String username, String password) {
		return WebClient.builder()
				.filter(ExchangeFilterFunctions.basicAuthentication(username, password))
				.build();
	}

}
