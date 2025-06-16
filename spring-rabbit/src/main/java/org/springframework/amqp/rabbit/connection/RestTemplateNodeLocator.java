/*
 * Copyright 2022-present the original author or authors.
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hc.client5.http.auth.AuthCache;
import org.apache.hc.client5.http.impl.auth.BasicAuthCache;
import org.apache.hc.client5.http.impl.auth.BasicScheme;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHost;
import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriUtils;

/**
 * A {@link NodeLocator} using the {@link RestTemplate}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 3.0
 *
 */
public class RestTemplateNodeLocator implements NodeLocator<RestTemplate> {

	private final AuthCache authCache = new BasicAuthCache();

	private final AtomicBoolean authSchemeIsSetToCache = new AtomicBoolean(false);

	@Override
	public RestTemplate createClient(String userName, String password) {
		HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
		requestFactory.setHttpContextFactory((httpMethod, uri) -> {
			HttpClientContext context = HttpClientContext.create();
			context.setAuthCache(this.authCache);
			return context;
		});
		RestTemplate template = new RestTemplate(requestFactory);
		template.getInterceptors().add(new BasicAuthenticationInterceptor(userName, password));
		return template;
	}

	@Override
	public @Nullable Map<String, Object> restCall(RestTemplate client, String baseUri, String vhost, String queue) {

		URI theBaseUri = URI.create(baseUri);
		if (!this.authSchemeIsSetToCache.getAndSet(true)) {
			this.authCache.put(HttpHost.create(theBaseUri), new BasicScheme());
		}
		URI uri = theBaseUri
				.resolve("/api/queues/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/" + queue);
		ResponseEntity<Map<String, Object>> response =
				client.exchange(uri, HttpMethod.GET, null, new ParameterizedTypeReference<>() {

				});
		return response.getStatusCode().equals(HttpStatus.OK) ? response.getBody() : null;
	}

}
