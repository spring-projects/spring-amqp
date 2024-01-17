/*
 * Copyright 2002-2024 the original author or authors.
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

package org.springframework.amqp.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Oleg Zhurakousky
 */
public class SimpleAmqpHeaderMapperTests {

	@Test
	public void fromHeaders() {
		SimpleAmqpHeaderMapper headerMapper = new SimpleAmqpHeaderMapper();
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put(AmqpHeaders.APP_ID, "test.appId");
		headerMap.put(AmqpHeaders.CLUSTER_ID, "test.clusterId");
		headerMap.put(AmqpHeaders.CONTENT_ENCODING, "test.contentEncoding");
		headerMap.put(AmqpHeaders.CONTENT_LENGTH, 99L);
		headerMap.put(AmqpHeaders.CONTENT_TYPE, "test.contentType");
		String testCorrelationId = "foo";
		headerMap.put(AmqpHeaders.CORRELATION_ID, testCorrelationId);
		headerMap.put(AmqpHeaders.DELAY, 1234);
		headerMap.put(AmqpHeaders.DELIVERY_MODE, MessageDeliveryMode.NON_PERSISTENT);
		headerMap.put(AmqpHeaders.DELIVERY_TAG, 1234L);
		headerMap.put(AmqpHeaders.EXPIRATION, "test.expiration");
		headerMap.put(AmqpHeaders.MESSAGE_COUNT, 42);
		headerMap.put(AmqpHeaders.MESSAGE_ID, "test.messageId");
		headerMap.put(AmqpHeaders.RECEIVED_EXCHANGE, "test.receivedExchange");
		headerMap.put(AmqpHeaders.RECEIVED_ROUTING_KEY, "test.receivedRoutingKey");
		headerMap.put(AmqpHeaders.REPLY_TO, "test.replyTo");
		Date testTimestamp = new Date();
		headerMap.put(AmqpHeaders.TIMESTAMP, testTimestamp);
		headerMap.put(AmqpHeaders.TYPE, "test.type");
		headerMap.put(AmqpHeaders.USER_ID, "test.userId");
		headerMap.put(AmqpHeaders.SPRING_REPLY_CORRELATION, "test.correlation");
		headerMap.put(AmqpHeaders.SPRING_REPLY_TO_STACK, "test.replyTo2");
		MessageHeaders messageHeaders = new MessageHeaders(headerMap);
		MessageProperties amqpProperties = new MessageProperties();
		headerMapper.fromHeaders(messageHeaders, amqpProperties);
		Set<String> headerKeys = amqpProperties.getHeaders().keySet();
		for (String headerKey : headerKeys) {
			if (headerKey.startsWith(AmqpHeaders.PREFIX)) {
				fail("No headers with 'amqp_' prefix expected");
			}
		}
		assertThat(amqpProperties.getAppId()).isEqualTo("test.appId");
		assertThat(amqpProperties.getClusterId()).isEqualTo("test.clusterId");
		assertThat(amqpProperties.getContentEncoding()).isEqualTo("test.contentEncoding");
		assertThat(amqpProperties.getContentLength()).isEqualTo(99L);
		assertThat(amqpProperties.getContentType()).isEqualTo("test.contentType");
		assertThat(amqpProperties.getCorrelationId()).isEqualTo(testCorrelationId);
		assertThat(amqpProperties.getDeliveryMode()).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(amqpProperties.getDeliveryTag()).isEqualTo(1234L);
		assertThat(amqpProperties.getExpiration()).isEqualTo("test.expiration");
		assertThat(amqpProperties.getMessageCount()).isEqualTo(Integer.valueOf(42));
		assertThat(amqpProperties.getMessageId()).isEqualTo("test.messageId");
		assertThat(amqpProperties.getReceivedExchange()).isEqualTo("test.receivedExchange");
		assertThat(amqpProperties.getReceivedRoutingKey()).isEqualTo("test.receivedRoutingKey");
		assertThat(amqpProperties.getReplyTo()).isEqualTo("test.replyTo");
		assertThat(amqpProperties.getTimestamp()).isEqualTo(testTimestamp);
		assertThat(amqpProperties.getType()).isEqualTo("test.type");
		assertThat(amqpProperties.getUserId()).isEqualTo("test.userId");
		assertThat(amqpProperties.getDelay()).isEqualTo(Integer.valueOf(1234));
	}

	@Test
	public void fromHeadersWithLongDelay() {
		SimpleAmqpHeaderMapper headerMapper = new SimpleAmqpHeaderMapper();
		Map<String, Object> headerMap = new HashMap<>();
		headerMap.put(AmqpHeaders.DELAY, 1234L);
		MessageHeaders messageHeaders = new MessageHeaders(headerMap);
		MessageProperties amqpProperties = new MessageProperties();
		headerMapper.fromHeaders(messageHeaders, amqpProperties);
		assertThat(amqpProperties.getDelayLong()).isEqualTo(Long.valueOf(1234));

		amqpProperties.setDelayLong(5678L);
		assertThat(amqpProperties.getDelayLong()).isEqualTo(Long.valueOf(5678));

		amqpProperties.setDelay(123);
		assertThat(amqpProperties.getDelayLong()).isNull();


	}


	@Test
	public void fromHeadersWithContentTypeAsMediaType() {
		SimpleAmqpHeaderMapper headerMapper = new SimpleAmqpHeaderMapper();
		Map<String, Object> headerMap = new HashMap<String, Object>();

		headerMap.put(AmqpHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_HTML);

		MessageHeaders messageHeaders = new MessageHeaders(headerMap);
		MessageProperties amqpProperties = new MessageProperties();
		headerMapper.fromHeaders(messageHeaders, amqpProperties);

		assertThat(amqpProperties.getContentType()).isEqualTo("text/html");
	}

	@Test
	public void toHeaders() {
		SimpleAmqpHeaderMapper headerMapper = new SimpleAmqpHeaderMapper();
		MessageProperties amqpProperties = new MessageProperties();
		amqpProperties.setAppId("test.appId");
		amqpProperties.setClusterId("test.clusterId");
		amqpProperties.setContentEncoding("test.contentEncoding");
		amqpProperties.setContentLength(99L);
		amqpProperties.setContentType("test.contentType");
		String testCorrelationId = "foo";
		amqpProperties.setCorrelationId(testCorrelationId);
		amqpProperties.setReceivedDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		amqpProperties.setDeliveryTag(1234L);
		amqpProperties.setExpiration("test.expiration");
		amqpProperties.setMessageCount(42);
		amqpProperties.setMessageId("test.messageId");
		amqpProperties.setPriority(22);
		amqpProperties.setReceivedDelay(1234);
		amqpProperties.setReceivedExchange("test.receivedExchange");
		amqpProperties.setReceivedRoutingKey("test.receivedRoutingKey");
		amqpProperties.setRedelivered(true);
		amqpProperties.setReplyTo("test.replyTo");
		Date testTimestamp = new Date();
		amqpProperties.setTimestamp(testTimestamp);
		amqpProperties.setType("test.type");
		amqpProperties.setReceivedUserId("test.userId");
		amqpProperties.setConsumerTag("consumer.tag");
		amqpProperties.setConsumerQueue("consumer.queue");
		amqpProperties.setHeader(AmqpHeaders.SPRING_REPLY_CORRELATION, "test.correlation");
		amqpProperties.setHeader(AmqpHeaders.SPRING_REPLY_TO_STACK, "test.replyTo2");
		Map<String, Object> headerMap = headerMapper.toHeaders(amqpProperties);
		assertThat(headerMap.get(AmqpHeaders.APP_ID)).isEqualTo("test.appId");
		assertThat(headerMap.get(AmqpHeaders.CLUSTER_ID)).isEqualTo("test.clusterId");
		assertThat(headerMap.get(AmqpHeaders.CONTENT_ENCODING)).isEqualTo("test.contentEncoding");
		assertThat(headerMap.get(AmqpHeaders.CONTENT_LENGTH)).isEqualTo(99L);
		assertThat(headerMap.get(AmqpHeaders.CONTENT_TYPE)).isEqualTo("test.contentType");
		assertThat(headerMap.get(AmqpHeaders.CORRELATION_ID)).isEqualTo(testCorrelationId);
		assertThat(headerMap.get(AmqpHeaders.RECEIVED_DELIVERY_MODE)).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(headerMap.get(AmqpHeaders.DELIVERY_TAG)).isEqualTo(1234L);
		assertThat(headerMap.get(AmqpHeaders.EXPIRATION)).isEqualTo("test.expiration");
		assertThat(headerMap.get(AmqpHeaders.MESSAGE_COUNT)).isEqualTo(42);
		assertThat(headerMap.get(AmqpHeaders.MESSAGE_ID)).isEqualTo("test.messageId");
		assertThat(headerMap.get(AmqpHeaders.RECEIVED_DELAY)).isEqualTo(1234);
		assertThat(headerMap.get(AmqpHeaders.RECEIVED_EXCHANGE)).isEqualTo("test.receivedExchange");
		assertThat(headerMap.get(AmqpHeaders.RECEIVED_ROUTING_KEY)).isEqualTo("test.receivedRoutingKey");
		assertThat(headerMap.get(AmqpHeaders.REPLY_TO)).isEqualTo("test.replyTo");
		assertThat(headerMap.get(AmqpHeaders.TIMESTAMP)).isEqualTo(testTimestamp);
		assertThat(headerMap.get(AmqpHeaders.TYPE)).isEqualTo("test.type");
		assertThat(headerMap.get(AmqpHeaders.RECEIVED_USER_ID)).isEqualTo("test.userId");
		assertThat(headerMap.get(AmqpHeaders.SPRING_REPLY_CORRELATION)).isEqualTo("test.correlation");
		assertThat(headerMap.get(AmqpHeaders.SPRING_REPLY_TO_STACK)).isEqualTo("test.replyTo2");
		assertThat(headerMap.get(AmqpHeaders.CONSUMER_TAG)).isEqualTo("consumer.tag");
		assertThat(headerMap.get(AmqpHeaders.CONSUMER_QUEUE)).isEqualTo("consumer.queue");
	}

	@Test // INT-2090
	public void jsonTypeIdNotOverwritten() {
		SimpleAmqpHeaderMapper headerMapper = new SimpleAmqpHeaderMapper();
		Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
		MessageProperties amqpProperties = new MessageProperties();
		converter.toMessage("123", amqpProperties);
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put("__TypeId__", "java.lang.Integer");
		MessageHeaders messageHeaders = new MessageHeaders(headerMap);
		headerMapper.fromHeaders(messageHeaders, amqpProperties);
		assertThat(amqpProperties.getHeaders().get("__TypeId__")).isEqualTo("java.lang.String");
		Object result = converter.fromMessage(new Message("123".getBytes(), amqpProperties));
		assertThat(result.getClass()).isEqualTo(String.class);
	}

}
