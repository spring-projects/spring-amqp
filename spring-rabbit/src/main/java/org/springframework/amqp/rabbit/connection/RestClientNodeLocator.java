/*
 * Copyright 2026-present the original author or authors.
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

import java.net.URI;
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
	public @Nullable Map<String, Object> restCall(RestClient client, String baseUri, String vhost, String queue) {

		URI uri = URI.create(baseUri)
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
