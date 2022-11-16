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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.hc.client5.http.auth.AuthCache;
import org.apache.hc.client5.http.impl.auth.BasicAuthCache;
import org.apache.hc.client5.http.impl.auth.BasicScheme;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.protocol.BasicHttpContext;
import org.apache.hc.core5.http.protocol.HttpContext;

import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.lang.Nullable;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriUtils;

/**
 * A {@link NodeLocator} using the {@link RestTemplate}.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class RestTemplateNodeLocator implements NodeLocator<RestTemplateHolder> {

	@Override
	public RestTemplateHolder createClient(String userName, String password) {
		return new RestTemplateHolder(userName, password);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	@Nullable
	public Map<String, Object> restCall(RestTemplateHolder client, String baseUri, String vhost, String queue)
			throws URISyntaxException {

		if (client.template == null) {
			URI uri = new URI(baseUri);
			HttpHost host = new HttpHost(uri.getHost(), uri.getPort());
			client.template = new RestTemplate(new HttpComponentsClientHttpRequestFactory() {

				@Override
				@Nullable
				protected HttpContext createHttpContext(HttpMethod httpMethod, URI uri) {
					AuthCache cache = new BasicAuthCache();
					BasicScheme scheme = new BasicScheme();
					cache.put(host, scheme);
					BasicHttpContext context = new BasicHttpContext();
					context.setAttribute(HttpClientContext.AUTH_CACHE, cache);
					return context;
				}

			});
			client.template.getInterceptors().add(new BasicAuthenticationInterceptor(client.userName, client.password));
		}
		URI uri = new URI(baseUri)
				.resolve("/api/queues/" + UriUtils.encodePathSegment(vhost, StandardCharsets.UTF_8) + "/" + queue);
		ResponseEntity<Map> response = client.template.exchange(uri, HttpMethod.GET, null, Map.class);
		return response.getStatusCode().equals(HttpStatus.OK) ? response.getBody() : null;
	}

	@Override
	public void close(RestTemplateHolder client) {
		try {
			client.template.close();
		}
		catch (IOException e) {
		}
	}

}
