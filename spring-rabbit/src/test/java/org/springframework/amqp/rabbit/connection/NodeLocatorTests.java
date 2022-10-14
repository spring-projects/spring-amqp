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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URISyntaxException;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.LocalizedQueueConnectionFactory.NodeLocator;
import org.springframework.lang.Nullable;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
public class NodeLocatorTests {

	@Test
	@DisplayName("don't exit early when node to address missing")
	void missingNode() throws URISyntaxException {

		NodeLocator<Object> nodeLocator = spy(new NodeLocator<Object>() {

			@Override
			public Object createClient(String userName, String password) {
				return null;
			}

			@Override
			@Nullable
			public Map<String, Object> restCall(Object client, String baseUri, String vhost, String queue) {
				if (baseUri.contains("foo")) {
					return Map.of("node", "c@d");
				}
				else {
					return Map.of("node", "a@b");
				}
			}

		});
		ConnectionFactory factory = nodeLocator.locate(new String[] { "http://foo", "http://bar" },
				Map.of("a@b", "baz"), null, "q", null, null, (q, n, u) -> {
					return null;
		});
		verify(nodeLocator, times(2)).restCall(any(), any(), any(), any());
	}

	@Test
	@DisplayName("rest returned null")
	void nullInfo() throws URISyntaxException {

		NodeLocator<Object> nodeLocator = spy(new NodeLocator<Object>() {

			@Override
			public Object createClient(String userName, String password) {
				return null;
			}

			@Override
			@Nullable
			public Map<String, Object> restCall(Object client, String baseUri, String vhost, String queue) {
				if (baseUri.contains("foo")) {
					return null;
				}
				else {
					return Map.of("node", "a@b");
				}
			}

		});
		ConnectionFactory factory = nodeLocator.locate(new String[] { "http://foo", "http://bar" },
				Map.of("a@b", "baz"), null, "q", null, null, (q, n, u) -> {
					return null;
		});
		verify(nodeLocator, times(2)).restCall(any(), any(), any(), any());
	}

	@Test
	@DisplayName("queue not found")
	void notFound() throws URISyntaxException {

		NodeLocator<Object> nodeLocator = spy(new NodeLocator<Object>() {

			@Override
			public Object createClient(String userName, String password) {
				return null;
			}

			@Override
			@Nullable
			public Map<String, Object> restCall(Object client, String baseUri, String vhost, String queue) {
				return null;
			}

		});
		ConnectionFactory factory = nodeLocator.locate(new String[] { "http://foo", "http://bar" },
				Map.of("a@b", "baz"), null, "q", null, null, (q, n, u) -> {
					return null;
		});
		verify(nodeLocator, times(2)).restCall(any(), any(), any(), any());
	}

}
