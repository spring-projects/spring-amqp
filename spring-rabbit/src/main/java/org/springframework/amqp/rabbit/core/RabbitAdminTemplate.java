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
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;

import org.springframework.amqp.core.Binding;
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

	private volatile String charset = "UTF-8";

	public RabbitAdminTemplate() {
		this("http://localhost:15672/api/", "guest", "guest");
	}

	public RabbitAdminTemplate(String baseUri, String user, String password) {
		this(baseUri, new CredentialsProviderFactory(user, password).get());
	}

	public RabbitAdminTemplate(String baseUri, CredentialsProvider credsProvider) {
		HttpClient httpClient = HttpClients.custom()
				.setDefaultCredentialsProvider(credsProvider)
				.build();
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
	public List<Binding> exchangeBindings(String vhost, String exchange) {
		URI uri = encodeAndExpand(baseUri + "exchanges/{vhost}/{exchange}/bindings/source", vhost, exchange);
		List<Map<String, Object>> list = this.restTemplate.getForObject(uri, List.class);
		return bindingsMapToBindings(list);
	}

	private List<Binding> bindingsMapToBindings(List<Map<String, Object>> list) {
		List<Binding> bindings = new ArrayList<Binding>(list.size());
		for (Map<String, Object> map : list) {
			@SuppressWarnings("unchecked")
			Binding binding = new Binding((String) map.get("destination"),
					Binding.DestinationType.valueOf(((String) map.get("destination_type")).toUpperCase()),
					(String) map.get("source"),
					(String) map.get("routing_key"),
					(Map<String, Object>) map.get("arguments"),
					map);
			map.remove("destination");
			map.remove("destination_type");
			map.remove("source");
			map.remove("routine_key");
			map.remove("arguments");
			bindings.add(binding);
		}
		return bindings;
	}

	private URI encodeAndExpand(String uri, String... params) {
		URI outUri = null;
		try {
			for (int i = 0; i < params.length; i++) {
				params[i] = URLEncoder.encode(params[i], this.charset);
			}
			UriComponents components = UriComponentsBuilder
					.fromUriString(uri)
					.buildAndExpand((Object[]) params);
			outUri = new URI(components.toUriString());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return outUri;
	}

	private static class CredentialsProviderFactory {

		private final String user;

		private final String password;

		public CredentialsProviderFactory(String user, String password) {
			this.user = user;
			this.password = password;
		}

		public CredentialsProvider get() {
			CredentialsProvider credsProvider =  new BasicCredentialsProvider();
			credsProvider.setCredentials(
					new AuthScope(null, -1),
					new UsernamePasswordCredentials(user, password));
			return credsProvider;
		}

	}

}
