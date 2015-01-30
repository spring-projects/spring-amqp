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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.support.Policy;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * @author Gary Russell
 * @since 1.5
 *
 */
public class RabbitManagementTemplate {

	private final RestTemplate restTemplate;

	private final String baseUri;

	public RabbitManagementTemplate() {
		this("http://localhost:15672/api/", "guest", "guest");
	}

	public RabbitManagementTemplate(String baseUri, String user, String password) {
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(
				new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT),
				new UsernamePasswordCredentials(user, password));
		HttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
		// Set up pre-emptive basic Auth because it seems the rabbit plugin doesn't support challenge/response for PUT
		// Create AuthCache instance
		AuthCache authCache = new BasicAuthCache();
		// Generate BASIC scheme object and add it to the local; from the apache docs...
		// auth cache
		BasicScheme basicAuth = new BasicScheme();
		URI uri;
		try {
			uri = new URI(baseUri);
		}
		catch (URISyntaxException e) {
			throw new AmqpException("Invalid URI", e);
		}
		authCache.put(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()), basicAuth);
		// Add AuthCache to the execution context
		final HttpClientContext localContext = HttpClientContext.create();
		localContext.setAuthCache(authCache);
		RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient) {

			@Override
			protected HttpContext createHttpContext(HttpMethod httpMethod, URI uri) {
				return localContext;
			}

		});
		this.restTemplate = restTemplate;
		this.baseUri = baseUri;
	}

	public RabbitManagementTemplate(String baseUri, RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
		this.baseUri = baseUri;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> aliveness(String vhost) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("aliveness-test", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		return this.restTemplate.getForObject(uri, Map.class);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> overview() {
		return this.restTemplate.getForObject(baseUri + "overview", Map.class);
	}

	public List<Policy> policies() {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> policies = this.restTemplate.getForObject(this.baseUri + "policies", List.class);
		return policyMapsToPolicies(policies);
	}

	public List<Policy> policies(String vhost) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("policies", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> policies = this.restTemplate.getForObject(uri, List.class);
		return policyMapsToPolicies(policies);
	}

	public void addPolicy(Policy policy) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("policies", "{vhost}", "{name}")
				.buildAndExpand(policy.getVhost(), policy.getName()).encode().toUri();
		this.restTemplate.put(uri, policy);
	}

	public void removePolicy(String vhost, String name) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("policies", "{vhost}", "{name}")
				.buildAndExpand(vhost, name).encode().toUri();
		this.restTemplate.delete(uri);
	}

	public List<Exchange> exchanges() {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> exchangeMaps = this.restTemplate.getForObject(baseUri + "exchanges", List.class);
		return exchangeMapsToExchanges(exchangeMaps);
	}

	@SuppressWarnings("unchecked")
	public List<Exchange> exchanges(String vhost) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("exchanges", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		List<Map<String, Object>> exchangeMaps = this.restTemplate.getForObject(uri, List.class);
		return exchangeMapsToExchanges(exchangeMaps);
	}

	public Exchange exchange(String vhost, String exchange) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("exchanges", "{vhost}", "{exchange}")
				.buildAndExpand(vhost, exchange).encode().toUri();
		@SuppressWarnings("unchecked")
		Map<String, Object> map = this.restTemplate.getForObject(uri, Map.class);
		return exchangeMapToExchange(map);
	}

	public List<Queue> queues() {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> queueMaps = this.restTemplate.getForObject(baseUri + "queues", List.class);
		return queueMapsToQueues(queueMaps);
	}

	public List<Queue> queues(String vhost) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("queues", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> queueMaps = this.restTemplate.getForObject(uri, List.class);
		return queueMapsToQueues(queueMaps);
	}

	public Queue queue(String vhost, String exchange) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("queues", "{vhost}", "{queue}")
				.buildAndExpand(vhost, exchange).encode().toUri();
		@SuppressWarnings("unchecked")
		Map<String, Object> map = this.restTemplate.getForObject(uri, Map.class);
		return queueMapToQueue(map);
	}

	public List<Binding> bindings() {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> bindingMaps = this.restTemplate.getForObject(baseUri + "bindings", List.class);
		return bindingMapsToBindings(bindingMaps);
	}

	public List<Binding> bindings(String vhost) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("bindings", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> bindingMaps = this.restTemplate.getForObject(uri, List.class);
		return bindingMapsToBindings(bindingMaps);
	}

	public List<Binding> exchangeToQueueBindings(String vhost, String exchange, String queue) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri)
				.pathSegment("bindings", "{vhost}", "e", "{exchange}", "q", "{queue}")
				.buildAndExpand(vhost, exchange, queue).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> list = this.restTemplate.getForObject(uri, List.class);
		return bindingMapsToBindings(list);
	}

	public List<Binding> exchangeToExchangeBindings(String vhost, String source, String dest) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri)
				.pathSegment("bindings", "{vhost}", "e", "{source}", "e", "{dest}")
				.buildAndExpand(vhost, source, dest).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> list = this.restTemplate.getForObject(uri, List.class);
		return bindingMapsToBindings(list);
	}

	public List<Binding> exchangeSourceBindings(String vhost, String exchange) {
		return exchangeBindings(vhost, exchange, "source");
	}

	public List<Binding> exchangeDestBindings(String vhost, String exchange) {
		return exchangeBindings(vhost, exchange, "destination");
	}

	private List<Binding> exchangeBindings(String vhost, String exchange, String which) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri)
				.pathSegment("exchanges", "{vhost}", "{exchange}", "bindings", which)
				.buildAndExpand(vhost, exchange).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> list = this.restTemplate.getForObject(uri, List.class);
		return bindingMapsToBindings(list);
	}

	/**
	 * Use this to invoke any general GET operation. Useful when invoking methods not supported
	 * by a specific method on this class.
	 * @param returnType the return type, usually {@link List} or {@link Map}.
	 * @param pathSegments the path segments/variable holders, e.g. {@code new String[] { "connections", "{name}" }.
	 * @param uriVariables variables to insert into path place holders.
	 * @return the result.
	 */
	public <T> T executeGet(Class<T> returnType, String[] pathSegments, Object... uriVariables) {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment(pathSegments)
				.buildAndExpand(uriVariables).encode().toUri();
		return this.restTemplate.getForObject(uri, returnType);
	}

	public static List<Policy> policyMapsToPolicies(List<Map<String, Object>> policies) {
		List<Policy> list = new ArrayList<Policy>(policies.size());
		for (Map<String, Object> map : policies) {
			list.add(policyMapToPolicy(map));
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public static Policy policyMapToPolicy(Map<String, Object> map) {
		return new Policy((String) map.get("vhost"), (String) map.get("name"),
				(String) map.get("pattern"), (String) map.get("apply_to"), (Integer) map.get("priority"),
				(Map<String, Object>) map.get("definition"));
	}

	public static List<Exchange> exchangeMapsToExchanges(List<Map<String, Object>> exchangeMaps) {
		List<Exchange> exchanges = new ArrayList<Exchange>(exchangeMaps.size());
		for (Map<String, Object> map : exchangeMaps) {
			exchanges.add(exchangeMapToExchange(map));
		}
		return exchanges;
	}

	public static Exchange exchangeMapToExchange(Map<String, Object> map) {
		AbstractExchange exchange = null;
		final String type = (String) map.get("type");
		String name = (String) map.get("name");
		boolean durable = (Boolean) map.get("durable");
		boolean autoDelete = (Boolean) map.get("auto_delete");
		@SuppressWarnings("unchecked")
		Map<String, Object> arguments = (Map<String, Object>) map.get("arguments");
		map.remove("type");
		map.remove("name");
		map.remove("durable");
		map.remove("auto_delete");
		map.remove("arguments");
		if (type.equals("direct")) {
			exchange = new DirectExchange(name, durable, autoDelete, arguments, map);
		}
		else if (type.equals("topic")) {
			exchange = new TopicExchange(name, durable, autoDelete, arguments, map);
		}
		else if (type.equals("fanout")) {
			exchange = new FanoutExchange(name, durable, autoDelete, arguments, map);
		}
		else if (type.equals("headers")) {
			exchange = new HeadersExchange(name, durable, autoDelete, arguments, map);
		}
		else {
			class UnknownExchange extends AbstractExchange {

				public UnknownExchange(String name, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
					super(name, durable, autoDelete, arguments);
				}

				@Override
				public String getType() {
					return type;
				}

			}
			exchange = new UnknownExchange(name, durable, autoDelete, arguments);
		}
		return exchange;
	}

	public static List<Queue> queueMapsToQueues(List<Map<String, Object>> queueMaps) {
		List<Queue> queues = new ArrayList<Queue>(queueMaps.size());
		for (Map<String, Object> map : queueMaps) {
			queues.add(queueMapToQueue(map));
		}
		return queues;
	}

	public static Queue queueMapToQueue(Map<String, Object> map) {
		String name = (String) map.get("name");
		boolean durable = (Boolean) map.get("durable");
		boolean exclusive = false; // not returned (Boolean) map.get("exclusive");
		boolean autoDelete = (Boolean) map.get("auto_delete");
		@SuppressWarnings("unchecked")
		Map<String, Object> arguments = (Map<String, Object>) map.get("arguments");
		map.remove("name");
		map.remove("durable");
		map.remove("auto_delete");
		map.remove("arguments");
		return new Queue(name, durable, exclusive, autoDelete, arguments, map);
	}

	public static List<Binding> bindingMapsToBindings(List<Map<String, Object>> list) {
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

}
