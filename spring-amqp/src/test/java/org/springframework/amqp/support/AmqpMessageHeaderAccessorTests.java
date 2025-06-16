/*
 * Copyright 2002-present the original author or authors.
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

import java.util.Date;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class AmqpMessageHeaderAccessorTests {

	@Test
	public void validateAmqpHeaders() {
		String correlationId = "correlation-id-1234";
		Date timestamp = new Date();

		MessageProperties properties = new MessageProperties();
		properties.setAppId("app-id-1234");
		properties.setClusterId("cluster-id-1234");
		properties.setContentEncoding("UTF-16");
		properties.setContentLength(200L);
		properties.setContentType("text/plain");
		properties.setCorrelationId(correlationId);
		properties.setReceivedDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		properties.setDeliveryTag(555L);
		properties.setExpiration("expiration-1234");
		properties.setMessageCount(42);
		properties.setMessageId("message-id-1234");
		properties.setPriority(9);
		properties.setReceivedExchange("received-exchange-1234");
		properties.setReceivedRoutingKey("received-routing-key-1234");
		properties.setRedelivered(true);
		properties.setReplyTo("reply-to-1234");
		properties.setTimestamp(timestamp);
		properties.setType("type-1234");
		properties.setReceivedUserId("user-id-1234");

		SimpleAmqpHeaderMapper amqpHeaderMapper = new SimpleAmqpHeaderMapper();

		Map<String, Object> mappedHeaders = amqpHeaderMapper.toHeaders(properties);
		Message<String> message = MessageBuilder.withPayload("test").copyHeaders(mappedHeaders).build();
		AmqpMessageHeaderAccessor headerAccessor = AmqpMessageHeaderAccessor.wrap(message);

		assertThat(headerAccessor.getAppId()).isEqualTo("app-id-1234");
		assertThat(headerAccessor.getClusterId()).isEqualTo("cluster-id-1234");
		assertThat(headerAccessor.getContentEncoding()).isEqualTo("UTF-16");
		assertThat(headerAccessor.getContentLength()).isEqualTo(Long.valueOf(200));
		assertThat(headerAccessor.getContentType()).isEqualTo(MimeType.valueOf("text/plain"));
		assertThat(headerAccessor.getCorrelationId()).isEqualTo(correlationId);
		assertThat(headerAccessor.getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(headerAccessor.getDeliveryTag()).isEqualTo(Long.valueOf(555));
		assertThat(headerAccessor.getExpiration()).isEqualTo("expiration-1234");
		assertThat(headerAccessor.getMessageCount()).isEqualTo(Integer.valueOf(42));
		assertThat(headerAccessor.getMessageId()).isEqualTo("message-id-1234");
		assertThat(headerAccessor.getPriority()).isEqualTo(Integer.valueOf(9));
		assertThat(headerAccessor.getReceivedExchange()).isEqualTo("received-exchange-1234");
		assertThat(headerAccessor.getReceivedRoutingKey()).isEqualTo("received-routing-key-1234");
		assertThat(headerAccessor.getRedelivered()).isEqualTo(true);
		assertThat(headerAccessor.getReplyTo()).isEqualTo("reply-to-1234");
		assertThat(headerAccessor.getTimestamp()).isEqualTo(Long.valueOf(timestamp.getTime()));
		assertThat(headerAccessor.getType()).isEqualTo("type-1234");
		assertThat(headerAccessor.getReceivedUserId()).isEqualTo("user-id-1234");

		// Making sure replyChannel is not mixed with replyTo
		assertThat(headerAccessor.getReplyChannel()).isNull();
	}

	@Test
	public void prioritySet() {
		Message<?> message = MessageBuilder.withPayload("payload").
				setHeader(AmqpMessageHeaderAccessor.PRIORITY, 90).build();
		AmqpMessageHeaderAccessor accessor = new AmqpMessageHeaderAccessor(message);
		assertThat(accessor.getPriority()).isEqualTo(Integer.valueOf(90));
	}

	@Test
	public void priorityMustBeInteger() {
		AmqpMessageHeaderAccessor accessor = new AmqpMessageHeaderAccessor(MessageBuilder.withPayload("foo").build());
		assertThatIllegalArgumentException()
				.isThrownBy(() -> accessor.setHeader(AmqpMessageHeaderAccessor.PRIORITY, "Foo"))
				.withFailMessage("priority");
	}

}
