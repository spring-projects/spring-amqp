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

package org.springframework.amqp.rabbit.connection;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.lang.Nullable;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;

/**
 * A {@link NodeLocator} using the Spring WebFlux {@link WebClient}.
 *
 * @author Gary Russell
 * @since 2.4.8
 *
 */
public class WebFluxNodeLocator implements NodeLocator<WebClient> {

	@Override
	@Nullable
	public Map<String, Object> restCall(WebClient client, String baseUri, String vhost, String queue)
			throws URISyntaxException {

		URI uri = new URI(baseUri)
				.resolve("/api/queues/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/" + queue);
		HashMap<String, Object> queueInfo = client.get()
				.uri(uri)
				.accept(MediaType.APPLICATION_JSON)
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<HashMap<String, Object>>() {
				})
				.block(Duration.ofSeconds(10)); // NOSONAR magic#
		return queueInfo != null ? queueInfo : null;
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
