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
import java.util.Collections;
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
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Use this class to interract with the RabbitMQ management plugin over its REST
 * API. It does NOT support messaging over that API or declaring/removing queues, exchanges
 * or bindings. Use {@link RabbitAdmin} for declaring/removing entities and the
 * {@link RabbitTemplate} or a message listener container for messaging.
 *
 * @author Gary Russell
 * @since 1.5
 */
public class RabbitManagementTemplate {

	private final RestTemplate restTemplate;

	private final String baseUri;

	/**
	 * Create a template with the default base URI and credentials.
	 */
	public RabbitManagementTemplate() {
		this("http://localhost:15672/api/", "guest", "guest");
	}

	/**
	 * Create a template with the supplied base URI and credentials.
	 * @param baseUri the uri with the form {@code http[s]://<host>:<port>/api/};
	 * default {@code http://localhost:15672/api/}.
	 * @param user the user.
	 * @param password the password.
	 */
	public RabbitManagementTemplate(String baseUri, String user, String password) {
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(
				new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT),
				new UsernamePasswordCredentials(user, password));
		HttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
		// Set up pre-emptive basic Auth because the rabbit plugin doesn't currently support challenge/response for PUT
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
		restTemplate.setMessageConverters(Collections.<HttpMessageConverter<?>>singletonList(
				new MappingJackson2HttpMessageConverter()));
		this.restTemplate = restTemplate;
		this.baseUri = baseUri;
	}

	/**
	 * Invoke the {@code /aliveness/<vhost>} method with the default '/' vhost.
	 * @return A map of keys.values; currently &#123;status: ok&#125; for success.
	 * @throws HttpClientErrorException the exception.
	 */
	public Map<String, Object> aliveness() throws HttpClientErrorException {
		return aliveness("/");
	}

	/**
	 * Invoke the {@code /aliveness/<vhost>}.
	 * @param vhost the vhost.
	 * @return A map of keys/values; currently &#123;status: ok&#125; for success.
	 * @throws HttpClientErrorException the exception.
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> aliveness(String vhost) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("aliveness-test", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		return this.restTemplate.getForObject(uri, Map.class);
	}

	/**
	 * Invoke the {@code /overview} method.
	 * @return A map of keys/values.
	 * @throws HttpClientErrorException the exception.
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> overview() throws HttpClientErrorException {
		return this.restTemplate.getForObject(baseUri + "overview", Map.class);
	}

	/**
	 * Invoke the {@code /policies} method.
	 * @return A list of all {@link Policy}s.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Policy> policies() throws HttpClientErrorException {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> policies = this.restTemplate.getForObject(this.baseUri + "policies", List.class);
		return policyMapsToPolicies(policies);
	}

	/**
	 * Invoke the {@code /policies/<vhost>} method.
	 * @param vhost the vhost.
	 * @return A list of all {@link Policy}s for the vhost.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Policy> policies(String vhost) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("policies", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> policies = this.restTemplate.getForObject(uri, List.class);
		return policyMapsToPolicies(policies);
	}

	/**
	 * Add a new {@link Policy}.
	 * @param policy the policy.
	 * @throws HttpClientErrorException the exception.
	 */
	public void addPolicy(Policy policy) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("policies", "{vhost}", "{name}")
				.buildAndExpand(policy.getVhost(), policy.getName()).encode().toUri();
		this.restTemplate.put(uri, policy);
	}

	/**
	 * Remove a {@link Policy} from the vhost.
	 * @param vhost the vhost.
	 * @param name the policy name.
	 * @throws HttpClientErrorException the exception.
	 */
	public void removePolicy(String vhost, String name) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("policies", "{vhost}", "{name}")
				.buildAndExpand(vhost, name).encode().toUri();
		this.restTemplate.delete(uri);
	}

	/**
	 * Invoke the {@code /exchanges} method.
	 * @return A list of all {@link Exchange}s.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Exchange> exchanges() throws HttpClientErrorException {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> exchangeMaps = this.restTemplate.getForObject(baseUri + "exchanges", List.class);
		return exchangeMapsToExchanges(exchangeMaps);
	}

	/**
	 * Invoke the {@code /exchanges/<vhost>} method.
	 * @param vhost the vhost.
	 * @return A list of all {@link Exchange}s for the vhost.
	 * @throws HttpClientErrorException the exception.
	 */
	@SuppressWarnings("unchecked")
	public List<Exchange> exchanges(String vhost) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("exchanges", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		List<Map<String, Object>> exchangeMaps = this.restTemplate.getForObject(uri, List.class);
		return exchangeMapsToExchanges(exchangeMaps);
	}

	/**
	 * Invoke the {@code /exchanges/<vhost>/<exchange>} method.
	 * @param vhost the vhost.
	 * @param exchange the exchange name.
	 * @return the exchange.
	 * @throws HttpClientErrorException the exception.
	 */
	public Exchange exchange(String vhost, String exchange) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("exchanges", "{vhost}", "{exchange}")
				.buildAndExpand(vhost, exchange).encode().toUri();
		@SuppressWarnings("unchecked")
		Map<String, Object> map = this.restTemplate.getForObject(uri, Map.class);
		return exchangeMapToExchange(map);
	}

	/**
	 * Invoke the {@code /queues} method.
	 * @return A list of all {@link Queues}s.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Queue> queues() throws HttpClientErrorException {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> queueMaps = this.restTemplate.getForObject(baseUri + "queues", List.class);
		return queueMapsToQueues(queueMaps);
	}

	/**
	 * Invoke the {@code /queues/<vhost>} method.
	 * @param vhost the vhost.
	 * @return A list of all {@link Queues}s for the vhost.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Queue> queues(String vhost) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("queues", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> queueMaps = this.restTemplate.getForObject(uri, List.class);
		return queueMapsToQueues(queueMaps);
	}

	/**
	 * Invoke the {@code /queue/<vhost>/<queue>} method.
	 * @param vhost the vhost.
	 * @param queue the queue name.
	 * @return the queue.
	 * @throws HttpClientErrorException the exception.
	 */
	public Queue queue(String vhost, String queue) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("queues", "{vhost}", "{queue}")
				.buildAndExpand(vhost, queue).encode().toUri();
		@SuppressWarnings("unchecked")
		Map<String, Object> map = this.restTemplate.getForObject(uri, Map.class);
		return queueMapToQueue(map);
	}

	/**
	 * Invoke the {@code /bindings} method.
	 * @return A list of all {@link Binding}s.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Binding> bindings() throws HttpClientErrorException {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> bindingMaps = this.restTemplate.getForObject(baseUri + "bindings", List.class);
		return bindingMapsToBindings(bindingMaps);
	}

	/**
	 * Invoke the {@code /bindings/<vhost>} method.
	 * @param vhost the vhost.
	 * @return A list of all {@link Binding}s for the vhost.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Binding> bindings(String vhost) throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment("bindings", "{vhost}")
				.buildAndExpand(vhost).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> bindingMaps = this.restTemplate.getForObject(uri, List.class);
		return bindingMapsToBindings(bindingMaps);
	}

	/**
	 * Invoke the {@code /bindings/<vhost>/e/<exhange>/q/<queue>} method.
	 * @param vhost the vhost.
	 * @param exchange the exchange name.
	 * @param queue the queue name.
	 * @return a list of the {@link Binding}s from the exchange to the queue.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Binding> exchangeToQueueBindings(String vhost, String exchange, String queue)
			throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri)
				.pathSegment("bindings", "{vhost}", "e", "{exchange}", "q", "{queue}")
				.buildAndExpand(vhost, exchange, queue).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> list = this.restTemplate.getForObject(uri, List.class);
		return bindingMapsToBindings(list);
	}

	/**
	 * Invoke the {@code /bindings/<vhost>/e/<exhange>/e/<destination>} method.
	 * @param vhost the vhost.
	 * @param source the source exchange name.
	 * @param dest the destination exchange name.
	 * @return a list of the {@link Binding}s from the source exchange to the destination exchange.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Binding> exchangeToExchangeBindings(String vhost, String source, String dest)
			throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri)
				.pathSegment("bindings", "{vhost}", "e", "{source}", "e", "{dest}")
				.buildAndExpand(vhost, source, dest).encode().toUri();
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> list = this.restTemplate.getForObject(uri, List.class);
		return bindingMapsToBindings(list);
	}

	/**
	 * Invoke the {@code /bindings/<vhost>/<exhange>/source} method.
	 * @param vhost the vhost.
	 * @param exchange the source exchange name.
	 * @return a list of all the {@link Binding}s for the source exchange.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Binding> exchangeSourceBindings(String vhost, String exchange) throws HttpClientErrorException {
		return exchangeBindings(vhost, exchange, "source");
	}

	/**
	 * Invoke the {@code /bindings/<vhost>/<exhange>/destination} method.
	 * @param vhost the vhost.
	 * @param exchange the destination exchange name.
	 * @return a list of all the {@link Binding}s where the exchange is the destination.
	 * @throws HttpClientErrorException the exception.
	 */
	public List<Binding> exchangeDestBindings(String vhost, String exchange) throws HttpClientErrorException {
		return exchangeBindings(vhost, exchange, "destination");
	}

	private List<Binding> exchangeBindings(String vhost, String exchange, String which)
			throws HttpClientErrorException {
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
	 * @param pathSegments the path segments/variable place holders, e.g. {@code new String[] { "connections", "{name}" }.
	 * @param uriVariables variables to insert into path place holders.
	 * @return the result.
	 * @throws HttpClientErrorException the exception.
	 */
	public <T> T executeGet(Class<T> returnType, String[] pathSegments, Object... uriVariables)
			throws HttpClientErrorException {
		URI uri = UriComponentsBuilder.fromUriString(this.baseUri).pathSegment(pathSegments)
				.buildAndExpand(uriVariables).encode().toUri();
		return this.restTemplate.getForObject(uri, returnType);
	}

	/**
	 * A helper method to convert a list of maps containing policy information to {@link Policy} objects.
	 * @param policysMaps the list of maps.
	 * @return the list of policies.
	 */
	public static List<Policy> policyMapsToPolicies(List<Map<String, Object>> policysMaps) {
		List<Policy> list = new ArrayList<Policy>(policysMaps.size());
		for (Map<String, Object> map : policysMaps) {
			list.add(policyMapToPolicy(map));
		}
		return list;
	}

	/**
	 * A helper method to convert a map containing policy information to a {@link Policy} object.
	 * @param policyMap the map.
	 * @return the {@link Policy}.
	 */
	@SuppressWarnings("unchecked")
	public static Policy policyMapToPolicy(Map<String, Object> policyMap) {
		return new Policy((String) policyMap.get("vhost"), (String) policyMap.get("name"),
				(String) policyMap.get("pattern"), (String) policyMap.get("apply_to"), (Integer) policyMap.get("priority"),
				(Map<String, Object>) policyMap.get("definition"));
	}

	/**
	 * A helper method to convert a list of maps containing exchange information to {@link Exchange} objects.
	 * @param exchangeMaps the list of maps.
	 * @return the list of {@link Exchange}s.
	 */
	public static List<Exchange> exchangeMapsToExchanges(List<Map<String, Object>> exchangeMaps) {
		List<Exchange> exchanges = new ArrayList<Exchange>(exchangeMaps.size());
		for (Map<String, Object> map : exchangeMaps) {
			exchanges.add(exchangeMapToExchange(map));
		}
		return exchanges;
	}

	/**
	 * A helper method to convert a map containing policy information to a {@link Policy} object.
	 * @param exchangeMap the map.
	 * @return the {@link Exchange}.
	 */
	public static Exchange exchangeMapToExchange(Map<String, Object> exchangeMap) {
		AbstractExchange exchange = null;
		final String type = (String) exchangeMap.get("type");
		String name = (String) exchangeMap.get("name");
		boolean durable = (Boolean) exchangeMap.get("durable");
		boolean autoDelete = (Boolean) exchangeMap.get("auto_delete");
		@SuppressWarnings("unchecked")
		Map<String, Object> arguments = (Map<String, Object>) exchangeMap.get("arguments");
		exchangeMap.remove("type");
		exchangeMap.remove("name");
		exchangeMap.remove("durable");
		exchangeMap.remove("auto_delete");
		exchangeMap.remove("arguments");
		if (type.equals("direct")) {
			exchange = new DirectExchange(name, durable, autoDelete, arguments, exchangeMap);
		}
		else if (type.equals("topic")) {
			exchange = new TopicExchange(name, durable, autoDelete, arguments, exchangeMap);
		}
		else if (type.equals("fanout")) {
			exchange = new FanoutExchange(name, durable, autoDelete, arguments, exchangeMap);
		}
		else if (type.equals("headers")) {
			exchange = new HeadersExchange(name, durable, autoDelete, arguments, exchangeMap);
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

	/**
	 * A helper method to convert a list of maps containing exchange information to {@link Queue} objects.
	 * @param queueMaps the list of maps.
	 * @return the list of {@link Queue}s.
	 */
	public static List<Queue> queueMapsToQueues(List<Map<String, Object>> queueMaps) {
		List<Queue> queues = new ArrayList<Queue>(queueMaps.size());
		for (Map<String, Object> map : queueMaps) {
			queues.add(queueMapToQueue(map));
		}
		return queues;
	}

	/**
	 * A helper method to convert a map containing policy information to a {@link Policy} object.
	 * @param exchangeMap the map.
	 * @return the {@link Exchange}.
	 */
	public static Queue queueMapToQueue(Map<String, Object> map) {
		String name = (String) map.get("name");
		boolean durable = (Boolean) map.get("durable");
		boolean exclusive = map.get("owner_pid_details") != null;
		boolean autoDelete = (Boolean) map.get("auto_delete");
		@SuppressWarnings("unchecked")
		Map<String, Object> arguments = (Map<String, Object>) map.get("arguments");
		map.remove("name");
		map.remove("durable");
		map.remove("auto_delete");
		map.remove("arguments");
		return new Queue(name, durable, exclusive, autoDelete, arguments, map);
	}

	/**
	 * A helper method to convert a list of maps containing binding information to {@link Binding} objects.
	 * @param bindingMaps the list of maps.
	 * @return the list of {@link Binding}s.
	 */
	public static List<Binding> bindingMapsToBindings(List<Map<String, Object>> bindingMaps) {
		List<Binding> bindings = new ArrayList<Binding>(bindingMaps.size());
		for (Map<String, Object> map : bindingMaps) {
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
