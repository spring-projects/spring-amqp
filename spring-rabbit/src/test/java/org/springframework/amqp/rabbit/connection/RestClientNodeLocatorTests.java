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

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.match.MockRestRequestMatchers;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Rene Choi
 *
 * @since 4.2
 *
 */
public class RestClientNodeLocatorTests {

	private final RestClientNodeLocator nodeLocator = new RestClientNodeLocator();

	@Test
	void queueInfoIsRetrievedFromEncodedUri() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(MockRestRequestMatchers.requestTo("http://localhost:15672/api/queues/%2F/some%20queue"))
				.andExpect(MockRestRequestMatchers.method(HttpMethod.GET))
				.andExpect(MockRestRequestMatchers.header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE))
				.andRespond(MockRestResponseCreators.withSuccess("{\"node\":\"rabbit@host\"}",
						MediaType.APPLICATION_JSON));

		Map<String, Object> queueInfo =
				this.nodeLocator.restCall(builder.build(), "http://localhost:15672/api/queues/", "/", "some queue");

		assertThat(queueInfo).containsEntry("node", "rabbit@host");
		server.verify();
	}

	@Test
	void apiPathIsResolvedAgainstTheHost() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(MockRestRequestMatchers.requestTo("http://localhost:15672/api/queues/vhost/queue"))
				.andRespond(MockRestResponseCreators.withSuccess("{\"node\":\"rabbit@host\"}",
						MediaType.APPLICATION_JSON));

		Map<String, Object> queueInfo =
				this.nodeLocator.restCall(builder.build(), "http://localhost:15672/api/", "vhost", "queue");

		assertThat(queueInfo).containsEntry("node", "rabbit@host");
		server.verify();
	}

}
