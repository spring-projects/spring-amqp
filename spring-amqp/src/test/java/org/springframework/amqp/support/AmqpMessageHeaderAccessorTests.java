/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.support;

import java.util.Date;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;

import static org.junit.Assert.*;

/**
 * @author Stephane Nicoll
 */
public class AmqpMessageHeaderAccessorTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Test
	public void validateAmqpHeaders() throws Exception {
		byte[] correlationId = "correlation-id-1234".getBytes();
		Date timestamp = new Date();

		MessageProperties properties = new MessageProperties();
		properties.setAppId("app-id-1234");
		properties.setClusterId("cluster-id-1234");
		properties.setContentEncoding("UTF-16");
		properties.setContentLength(200L);
		properties.setContentType("text/plain");
		properties.setCorrelationId(correlationId);
		properties.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
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
		properties.setUserId("user-id-1234");

		SimpleAmqpHeaderMapper amqpHeaderMapper = new SimpleAmqpHeaderMapper();

		Map<String, Object> mappedHeaders = amqpHeaderMapper.toHeaders(properties);
		Message<String> message = MessageBuilder.withPayload("test").copyHeaders(mappedHeaders).build();
		AmqpMessageHeaderAccessor headerAccessor = AmqpMessageHeaderAccessor.wrap(message);

		assertEquals("app-id-1234", headerAccessor.getAppId());
		assertEquals("cluster-id-1234", headerAccessor.getClusterId());
		assertEquals("UTF-16", headerAccessor.getContentEncoding());
		assertEquals(Long.valueOf(200), headerAccessor.getContentLength());
		assertEquals(MimeType.valueOf("text/plain"), headerAccessor.getContentType());
		assertEquals(correlationId, headerAccessor.getCorrelationId());
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, headerAccessor.getDeliveryMode());
		assertEquals(Long.valueOf(555), headerAccessor.getDeliveryTag());
		assertEquals("expiration-1234", headerAccessor.getExpiration());
		assertEquals(Integer.valueOf(42), headerAccessor.getMessageCount());
		assertEquals("message-id-1234", headerAccessor.getMessageId());
		assertEquals(Integer.valueOf(9), headerAccessor.getPriority());
		assertEquals("received-exchange-1234", headerAccessor.getReceivedExchange());
		assertEquals("received-routing-key-1234", headerAccessor.getReceivedRoutingKey());
		assertEquals(true, headerAccessor.getRedelivered());
		assertEquals("reply-to-1234", headerAccessor.getReplyTo());
		assertEquals(Long.valueOf(timestamp.getTime()), headerAccessor.getTimestamp());
		assertEquals("type-1234", headerAccessor.getType());
		assertEquals("user-id-1234", headerAccessor.getUserId());

		// Making sure replyChannel is not mixed with replyTo
		assertNull(headerAccessor.getReplyChannel());
	}

	@Test
	public void prioritySet() {
		Message<?> message = MessageBuilder.withPayload("payload").
				setHeader(AmqpMessageHeaderAccessor.PRIORITY, 90).build();
		AmqpMessageHeaderAccessor accessor = new AmqpMessageHeaderAccessor(message);
		assertEquals(Integer.valueOf(90), accessor.getPriority());
	}

	@Test
	public void priorityMustBeInteger() {
		AmqpMessageHeaderAccessor accessor = new AmqpMessageHeaderAccessor(MessageBuilder.withPayload("foo").build());
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("priority");
		accessor.setHeader(AmqpMessageHeaderAccessor.PRIORITY, "Foo");
	}

}
