/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.amqp.core.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.junit.Test;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;

/**
 * @author Gary Russell
 * @author Alex Panchenko
 *
 * @since 1.3
 *
 */
public class MessageBuilderTests {

	@Test
	public void fromBodyAndMessage() {
		byte[] bytes = "foo".getBytes();
		MessageProperties properties = this.setAll(MessagePropertiesBuilder.newInstance())
				.setReplyTo("replyTo")
				.setReplyToIfAbsent("foo")
				.build();
		Message message1 = MessageBuilder.withBody(bytes)
				.andProperties(properties)
				.build();
		assertSame(bytes, message1.getBody());
		assertEquals("replyTo", message1.getMessageProperties().getReplyTo());

		Message message2 = MessageBuilder.fromMessage(message1)
				.setReplyTo("foo")
				.build();
		assertSame(bytes, message2.getBody());
		assertNotSame(message1.getMessageProperties(), message2.getMessageProperties());
		assertEquals(message1.getMessageProperties(),
				MessageBuilder.fromMessage(message2).setReplyTo("replyTo").build().getMessageProperties());
		assertEquals("foo", message2.getMessageProperties().getReplyTo());

		Message message3 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToIfAbsent("foo")
				.build();
		assertEquals("replyTo", message3.getMessageProperties().getReplyTo());

		Message message4 = MessageBuilder.fromClonedMessage(message1)
				.setReplyTo(null)
				.setReplyToIfAbsent("foo")
				.build();
		assertEquals("foo", message4.getMessageProperties().getReplyTo());

	}

	@Test
	public void fromBodyAndMessageCloned() {
		byte[] bytes = "foo".getBytes();
		MessageProperties properties = new MessageProperties();
		Address replyTo = new Address("address");
		Message message1 = MessageBuilder.withClonedBody(bytes)
				.andProperties(this.setAll(MessagePropertiesBuilder.fromClonedProperties(properties))
						.setReplyToAddress(replyTo)
						.setReplyToAddressIfAbsent(new Address("addressxxxx"))
						.build())
				.build();
		assertNotSame(bytes, message1.getBody());
		assertTrue(Arrays.equals(bytes, message1.getBody()));
		assertEquals(replyTo.toString(), message1.getMessageProperties().getReplyToAddress().toString());

		Address foo = new Address("foo");
		Message message2 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToAddress(foo)
				.build();
		assertNotSame(message1.getBody(), message2.getBody());
		assertTrue(Arrays.equals(bytes, message2.getBody()));
		assertEquals(message1.getMessageProperties(), MessageBuilder.fromMessage(message2)
				.setReplyToAddress(replyTo).build().getMessageProperties());
		assertEquals(foo.toString(), message2.getMessageProperties().getReplyToAddress().toString());

		Message message3 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToAddressIfAbsent(foo)
				.build();
		assertEquals(replyTo.toString(), message3.getMessageProperties().getReplyToAddress().toString());

		Message message4 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToAddress(null)
				.setReplyToAddressIfAbsent(foo)
				.build();
		assertEquals(foo.toString(), message4.getMessageProperties().getReplyToAddress().toString());
	}

	@Test
	public void fromBodyAndMessageRange() {
		byte[] bytes = "foobar".getBytes();
		Message message1 = MessageBuilder.withBody(bytes, 2, 5)
				.andProperties(this.setAll(MessagePropertiesBuilder.newInstance())
						.build())
				.build();
		assertTrue(Arrays.equals("oba".getBytes(), message1.getBody()));

		Message message2 = MessageBuilder.fromClonedMessage(message1).build();
		assertNotSame(message1.getBody(), message2.getBody());
		assertTrue(Arrays.equals(message1.getBody(), message2.getBody()));
		assertEquals(message1.getMessageProperties(), message2.getMessageProperties());
	}

	@Test
	public void copyProperties() {
		byte[] bytes = "foo".getBytes();
		Message message1 = MessageBuilder.withBody(bytes)
				.andProperties(this.setAll(MessagePropertiesBuilder.newInstance())
					.setReplyTo("replyTo")
					.build())
				.build();
		assertSame(bytes, message1.getBody());

		Message message2 = MessageBuilder.withBody("bar".getBytes())
				.copyProperties(message1.getMessageProperties())
				.build();
		assertNotSame(message1.getMessageProperties(), message2.getMessageProperties());
		assertEquals(message1.getMessageProperties(), message2.getMessageProperties());

		Message message3 = MessageBuilder.withBody("bar".getBytes())
				.copyProperties(message1.getMessageProperties())
				.removeHeader("foo")
				.build();
		assertEquals(2, message3.getMessageProperties().getHeaders().size());

		Message message4 = MessageBuilder.withBody("bar".getBytes())
				.copyProperties(message1.getMessageProperties())
				.removeHeaders()
				.build();
		assertEquals(0, message4.getMessageProperties().getHeaders().size());
	}

	@Test
	public void ifAbsentNoneReplaced() {
		MessageProperties properties = new MessageProperties();
		MessagePropertiesBuilder builder = MessagePropertiesBuilder.fromProperties(properties);
		this.setAll(builder);
		this.setAllIfAbsent(builder);
		properties = builder.build();
		assertLower(properties);
	}

	@Test
	public void ifAbsentAllAdded() {
		MessagePropertiesBuilder builder = MessagePropertiesBuilder.newInstance();
		this.setAllIfAbsent(builder);
		MessageProperties properties = builder.build();
		assertUpper(properties);
	}

	@Test
	public void replaceAll() {
		MessageProperties properties = new MessageProperties();
		MessagePropertiesBuilder builder = MessagePropertiesBuilder.fromProperties(properties);
		this.setAllIfAbsent(builder);
		this.setAll(builder);
		properties = builder.build();
		assertLower(properties);
	}

	@Test
	public void ifAbsentNoneReplacedFluencyOnMessageBuilder() {
		MessageBuilder builder = MessageBuilder.withBody("foo".getBytes());
		this.setAll(builder);
		this.setAllIfAbsent(builder);
		MessageProperties properties = builder.build().getMessageProperties();
		assertLower(properties);
	}

	@Test
	public void ifAbsentAllAddedFluencyOnMessageBuilder() {
		MessageBuilder builder = MessageBuilder.withBody("foo".getBytes());
		this.setAllIfAbsent(builder);
		MessageProperties properties = builder.build().getMessageProperties();
		assertUpper(properties);
	}

	@Test
	public void replaceAllFluencyOnMessageBuilder() {
		MessageBuilder builder = MessageBuilder.withBody("foo".getBytes());
		this.setAllIfAbsent(builder);
		this.setAll(builder);
		MessageProperties properties = builder.build().getMessageProperties();
		assertLower(properties);
	}

	private MessagePropertiesBuilder setAll(MessagePropertiesBuilder builder) {
		builder.setAppId("appId")
			.setClusterId("clusterId")
			.setContentEncoding("contentEncoding")
			.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN)
			.setContentLength(1)
			.setCorrelationId("correlationId")
			.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT)
			.setDeliveryTag(2L)
			.setExpiration("expiration")
			.setHeader("foo", "bar")
			.copyHeaders(Collections.<String, Object>singletonMap("qux", "fiz"))
			.copyHeaders(Collections.<String, Object>singletonMap("baz", "fuz"))
			.setMessageCount(3)
			.setMessageId("messageId")
			.setPriority(4)
			.setReceivedExchange("receivedExchange")
			.setReceivedRoutingKey("receivedRoutingKey")
			.setRedelivered(true)
			.setTimestamp(new Date())
			.setType("type")
			.setUserId("userId");
		return builder;
	}

	private MessagePropertiesBuilder setAllIfAbsent(MessagePropertiesBuilder builder) {
		builder.setAppIdIfAbsent("APPID")
			.setClusterIdIfAbsent("CLUSTERID")
			.setContentEncodingIfAbsent("CONTENTENCODING")
			.setContentTypeIfAbsentOrDefault(MessageProperties.CONTENT_TYPE_BYTES)
			.setContentLengthIfAbsent(10)
			.setCorrelationIdIfAbsent("CORRELATIONID")
			.setDeliveryModeIfAbsentOrDefault(MessageDeliveryMode.PERSISTENT)
			.setDeliveryTagIfAbsent(20L)
			.setExpirationIfAbsent("EXPIRATION")
			.setHeaderIfAbsent("foo", "BAR")
			.copyHeadersIfAbsent(Collections.<String, Object>singletonMap("qux", "FIZ"))
			.copyHeadersIfAbsent(Collections.<String, Object>singletonMap("baz", "FUZ"))
			.setMessageCountIfAbsent(30)
			.setMessageIdIfAbsent("MESSAGEID")
			.setPriorityIfAbsentOrDefault(40)
			.setReceivedExchangeIfAbsent("RECEIVEDEXCHANGE")
			.setReceivedRoutingKeyIfAbsent("RECEIVEDROUTINGKEY")
			.setRedeliveredIfAbsent(false)
			.setTimestampIfAbsent(new Date(0))
			.setTypeIfAbsent("TYPE")
			.setUserIdIfAbsent("USERID");
		return builder;
	}

	private MessageBuilder setAll(MessageBuilder builder) {
		builder.setAppId("appId")
			.setClusterId("clusterId")
			.setContentEncoding("contentEncoding")
			.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN)
			.setContentLength(1)
			.setCorrelationId("correlationId")
			.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT)
			.setDeliveryTag(2L)
			.setExpiration("expiration")
			.setHeader("foo", "bar")
			.copyHeaders(Collections.<String, Object>singletonMap("qux", "fiz"))
			.copyHeaders(Collections.<String, Object>singletonMap("baz", "fuz"))
			.setMessageCount(3)
			.setMessageId("messageId")
			.setPriority(4)
			.setReceivedExchange("receivedExchange")
			.setReceivedRoutingKey("receivedRoutingKey")
			.setRedelivered(true)
			.setTimestamp(new Date())
			.setType("type")
			.setUserId("userId");
		return builder;
	}

	private MessageBuilder setAllIfAbsent(MessageBuilder builder) {
		builder.setAppIdIfAbsent("APPID")
			.setClusterIdIfAbsent("CLUSTERID")
			.setContentEncodingIfAbsent("CONTENTENCODING")
			.setContentTypeIfAbsentOrDefault(MessageProperties.CONTENT_TYPE_BYTES)
			.setContentLengthIfAbsent(10)
			.setCorrelationIdIfAbsent("CORRELATIONID")
			.setDeliveryModeIfAbsentOrDefault(MessageDeliveryMode.PERSISTENT)
			.setDeliveryTagIfAbsent(20L)
			.setExpirationIfAbsent("EXPIRATION")
			.setHeaderIfAbsent("foo", "BAR")
			.copyHeadersIfAbsent(Collections.<String, Object>singletonMap("qux", "FIZ"))
			.copyHeadersIfAbsent(Collections.<String, Object>singletonMap("baz", "FUZ"))
			.setMessageCountIfAbsent(30)
			.setMessageIdIfAbsent("MESSAGEID")
			.setPriorityIfAbsentOrDefault(40)
			.setReceivedExchangeIfAbsent("RECEIVEDEXCHANGE")
			.setReceivedRoutingKeyIfAbsent("RECEIVEDROUTINGKEY")
			.setRedeliveredIfAbsent(false)
			.setTimestampIfAbsent(new Date(0))
			.setTypeIfAbsent("TYPE")
			.setUserIdIfAbsent("USERID");
		return builder;
	}

	private void assertLower(MessageProperties properties) {
		assertEquals("appId", properties.getAppId());
		assertEquals("clusterId", properties.getClusterId());
		assertEquals("contentEncoding", properties.getContentEncoding());
		assertEquals(MessageProperties.CONTENT_TYPE_TEXT_PLAIN, properties.getContentType());
		assertEquals(1, properties.getContentLength());
		assertEquals("correlationId", properties.getCorrelationId());
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, properties.getDeliveryMode());
		assertEquals(2, properties.getDeliveryTag());
		assertEquals("expiration", properties.getExpiration());
		assertEquals("bar", properties.getHeaders().get("foo"));
		assertEquals("fiz", properties.getHeaders().get("qux"));
		assertEquals("fuz", properties.getHeaders().get("baz"));
		assertEquals(Integer.valueOf(3), properties.getMessageCount());
		assertEquals("messageId", properties.getMessageId());
		assertEquals(Integer.valueOf(4), properties.getPriority());
		assertEquals("receivedExchange", properties.getReceivedExchange());
		assertEquals("receivedRoutingKey", properties.getReceivedRoutingKey());
		assertTrue(properties.getRedelivered());
		assertTrue(properties.getTimestamp().getTime() > 0);
		assertEquals("type", properties.getType());
		assertEquals("userId", properties.getUserId());
	}

	private void assertUpper(MessageProperties properties) {
		assertEquals("APPID", properties.getAppId());
		assertEquals("CLUSTERID", properties.getClusterId());
		assertEquals("CONTENTENCODING", properties.getContentEncoding());
		assertEquals(MessageProperties.CONTENT_TYPE_BYTES, properties.getContentType());
		assertEquals(10, properties.getContentLength());
		assertEquals("CORRELATIONID", properties.getCorrelationId());
		assertEquals(MessageDeliveryMode.PERSISTENT, properties.getDeliveryMode());
		assertEquals(20, properties.getDeliveryTag());
		assertEquals("EXPIRATION", properties.getExpiration());
		assertEquals("BAR", properties.getHeaders().get("foo"));
		assertEquals("FIZ", properties.getHeaders().get("qux"));
		assertEquals("FUZ", properties.getHeaders().get("baz"));
		assertEquals(Integer.valueOf(30), properties.getMessageCount());
		assertEquals("MESSAGEID", properties.getMessageId());
		assertEquals(Integer.valueOf(40), properties.getPriority());
		assertEquals("RECEIVEDEXCHANGE", properties.getReceivedExchange());
		assertEquals("RECEIVEDROUTINGKEY", properties.getReceivedRoutingKey());
		assertFalse(properties.getRedelivered());
		assertTrue(properties.getTimestamp().getTime() == 0);
		assertEquals("TYPE", properties.getType());
		assertEquals("USERID", properties.getUserId());
	}

}
