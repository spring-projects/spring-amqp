/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriUtils;

/**
 * A {@link NodeLocator} using the {@link RestClient}.
 *
 * @author Rene Choi
 * @author Ngoc Nhaan
 *
 * @since 4.2
 *
 */
public class RestClientNodeLocator implements NodeLocator<RestClient> {

	@Override
	public RestClient createClient(String userName, String password) {
		return RestClient.builder()
				.requestInterceptor(new BasicAuthenticationInterceptor(userName, password))
				.build();
	}

	@Override
	public @Nullable Map<String, Object> restCall(RestClient client, String baseUri, String vhost, String queue)
			throws URISyntaxException {
		URI uri = new URI(baseUri)
				.resolve("/api/queues/"
						+ UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/"
						+ UriUtils.encodePathSegment(queue, StandardCharsets.UTF_8));

		return client.get()
				.uri(uri)
				.accept(MediaType.APPLICATION_JSON)
				.retrieve()
				.body(new ParameterizedTypeReference<>() {

				});
	}

}
