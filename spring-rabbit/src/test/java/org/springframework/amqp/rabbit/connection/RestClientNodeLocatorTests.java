/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.net.URISyntaxException;
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
 * @author Ngoc Nhaan
 *
 * @since 4.2
 *
 */
public class RestClientNodeLocatorTests {

	private final RestClientNodeLocator nodeLocator = new RestClientNodeLocator();

	@Test
	void queueInfoIsRetrievedFromEncodedUri() throws URISyntaxException {
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
	void apiPathIsResolvedAgainstTheHost() throws URISyntaxException {
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
