/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.Map;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;

import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * @author Gary Russell
 * @since 1.5
 *
 */
public class RabbitAdminTemplate {

	private final RestTemplate restTemplate;

	private final String baseUri;

	public RabbitAdminTemplate() {
		this("http://localhost:15672/api/", "guest", "guest");
	}

	public RabbitAdminTemplate(String baseUri, String user, String password) {
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(
				new AuthScope(null, -1),
				new UsernamePasswordCredentials(user, password));
		HttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
		RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient));
		this.restTemplate = restTemplate;
		this.baseUri = baseUri;
	}

	public RabbitAdminTemplate(String baseUri, RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
		this.baseUri = baseUri;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> overview() {
		return this.restTemplate.getForObject(baseUri + "overview", Map.class);
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> exchanges() {
		return this.restTemplate.getForObject(baseUri + "exchanges", List.class);
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> exchanges(String vhost) {
		URI uri = encodeAndExpand(baseUri + "exchanges/{vhost}", vhost);
		return this.restTemplate.getForObject(uri, List.class);
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> exchangeBindings(String vhost, String exchange) {
		URI uri = encodeAndExpand(baseUri + "exchanges/{vhost}/{exchange}/bindings/source", vhost, exchange);
		return this.restTemplate.getForObject(uri, List.class);
	}

	private URI encodeAndExpand(String uri, String... params) {
		for (int i = 0; i < params.length; i++) {
			params[i] = params[i].replaceAll("/", "%2f");
		}
		UriComponents components = UriComponentsBuilder.fromUriString(uri).buildAndExpand((Object[]) params);
		URI tempUri = components.toUri();
		String escaping = tempUri.toString().replaceAll("%252f", "%2f");
		try {
			tempUri = new URI(escaping);
		}
		catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return tempUri;
	}

}
