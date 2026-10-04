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

import java.net.URISyntaxException;
import java.util.Map;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Ngoc Nhaan
 *
 * @since 4.2
 */
class WebFluxNodeLocatorTest {

	private final WebFluxNodeLocator nodeLocator = new WebFluxNodeLocator();

	@StartStop
	private final MockWebServer mockWebServer = new MockWebServer();

	@Test
	void queueInfoIsRetrievedFromEncodedUri() throws URISyntaxException {

		MockResponse mockResponse = new MockResponse.Builder()
				.code(200)
				.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
				.body("{\"node\":\"rabbit@host\"}")
				.build();
		this.mockWebServer.enqueue(mockResponse);

		int port = this.mockWebServer.url("/").port();
		Map<String, Object> queueInfo = this.nodeLocator.restCall(WebClient.create(), "http://localhost:" + port, "/", "some queue");
		assertThat(queueInfo).containsEntry("node", "rabbit@host");
	}

	@Test
	void badRequestResponseThrowsException() {

		MockResponse mockResponse = new MockResponse.Builder()
				.code(400)
				.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
				.body("{\"error\":\"Object Not Found\",\"reason\":\"Not Found\"}")
				.build();
		this.mockWebServer.enqueue(mockResponse);

		int port = this.mockWebServer.url("/").port();
		assertThatExceptionOfType(WebClientResponseException.class)
				.isThrownBy(() -> this.nodeLocator.restCall(WebClient.create(), "http://localhost:" + port, "/", "error queue"))
				.withMessage("400 Bad Request from GET http://localhost:%s/api/queues/%%2F/error%%20queue".formatted(port));
	}

}
