/*
 * Copyright 2014-2025 the original author or authors.
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

package org.springframework.amqp.core.builder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;

import static org.assertj.core.api.Assertions.assertThat;

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
		assertThat(message1.getBody()).isSameAs(bytes);
		assertThat(message1.getMessageProperties().getReplyTo()).isEqualTo("replyTo");

		Message message2 = MessageBuilder.fromMessage(message1)
				.setReplyTo("foo")
				.build();
		assertThat(message2.getBody()).isSameAs(bytes);
		assertThat(message2.getMessageProperties()).isNotSameAs(message1.getMessageProperties());
		assertThat(MessageBuilder.fromMessage(message2).setReplyTo("replyTo").build().getMessageProperties()).isEqualTo(message1.getMessageProperties());
		assertThat(message2.getMessageProperties().getReplyTo()).isEqualTo("foo");

		Message message3 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToIfAbsent("foo")
				.build();
		assertThat(message3.getMessageProperties().getReplyTo()).isEqualTo("replyTo");

		Message message4 = MessageBuilder.fromClonedMessage(message1)
				.setReplyTo(null)
				.setReplyToIfAbsent("foo")
				.build();
		assertThat(message4.getMessageProperties().getReplyTo()).isEqualTo("foo");

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
		assertThat(message1.getBody()).isNotSameAs(bytes);
		assertThat(Arrays.equals(bytes, message1.getBody())).isTrue();
		assertThat(message1.getMessageProperties().getReplyToAddress().toString()).isEqualTo(replyTo.toString());

		Address foo = new Address("foo");
		Message message2 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToAddress(foo)
				.build();
		assertThat(message2.getBody()).isNotSameAs(message1.getBody());
		assertThat(Arrays.equals(bytes, message2.getBody())).isTrue();
		assertThat(MessageBuilder.fromMessage(message2)
				.setReplyToAddress(replyTo).build().getMessageProperties()).isEqualTo(message1.getMessageProperties());
		assertThat(message2.getMessageProperties().getReplyToAddress().toString()).isEqualTo(foo.toString());

		Message message3 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToAddressIfAbsent(foo)
				.build();
		assertThat(message3.getMessageProperties().getReplyToAddress().toString()).isEqualTo(replyTo.toString());

		Message message4 = MessageBuilder.fromClonedMessage(message1)
				.setReplyToAddress(null)
				.setReplyToAddressIfAbsent(foo)
				.build();
		assertThat(message4.getMessageProperties().getReplyToAddress().toString()).isEqualTo(foo.toString());
	}

	@Test
	public void fromBodyAndMessageRange() {
		byte[] bytes = "foobar".getBytes();
		Message message1 = MessageBuilder.withBody(bytes, 2, 5)
				.andProperties(this.setAll(MessagePropertiesBuilder.newInstance())
						.build())
				.build();
		assertThat(Arrays.equals("oba".getBytes(), message1.getBody())).isTrue();

		Message message2 = MessageBuilder.fromClonedMessage(message1).build();
		assertThat(message2.getBody()).isNotSameAs(message1.getBody());
		assertThat(Arrays.equals(message1.getBody(), message2.getBody())).isTrue();
		assertThat(message2.getMessageProperties()).isEqualTo(message1.getMessageProperties());
	}

	@Test
	public void copyProperties() {
		byte[] bytes = "foo".getBytes();
		Message message1 = MessageBuilder.withBody(bytes)
				.andProperties(this.setAll(MessagePropertiesBuilder.newInstance())
					.setReplyTo("replyTo")
					.build())
				.build();
		assertThat(message1.getBody()).isSameAs(bytes);

		Message message2 = MessageBuilder.withBody("bar".getBytes())
				.copyProperties(message1.getMessageProperties())
				.build();
		assertThat(message2.getMessageProperties()).isNotSameAs(message1.getMessageProperties());
		assertThat(message2.getMessageProperties()).isEqualTo(message1.getMessageProperties());

		Message message3 = MessageBuilder.withBody("bar".getBytes())
				.copyProperties(message1.getMessageProperties())
				.removeHeader("foo")
				.build();
		assertThat(message3.getMessageProperties().getHeaders()).hasSize(2);

		Message message4 = MessageBuilder.withBody("bar".getBytes())
				.copyProperties(message1.getMessageProperties())
				.removeHeaders()
				.build();
		assertThat(message4.getMessageProperties().getHeaders()).hasSize(0);
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
		assertThat(properties.getAppId()).isEqualTo("appId");
		assertThat(properties.getClusterId()).isEqualTo("clusterId");
		assertThat(properties.getContentEncoding()).isEqualTo("contentEncoding");
		assertThat(properties.getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		assertThat(properties.getContentLength()).isEqualTo(1);
		assertThat(properties.getCorrelationId()).isEqualTo("correlationId");
		assertThat(properties.getDeliveryMode()).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(properties.getDeliveryTag()).isEqualTo(2);
		assertThat(properties.getExpiration()).isEqualTo("expiration");
		assertThat(properties.getHeaders().get("foo")).isEqualTo("bar");
		assertThat(properties.getHeaders().get("qux")).isEqualTo("fiz");
		assertThat(properties.getHeaders().get("baz")).isEqualTo("fuz");
		assertThat(properties.getMessageCount()).isEqualTo(Integer.valueOf(3));
		assertThat(properties.getMessageId()).isEqualTo("messageId");
		assertThat(properties.getPriority()).isEqualTo(Integer.valueOf(4));
		assertThat(properties.getReceivedExchange()).isEqualTo("receivedExchange");
		assertThat(properties.getReceivedRoutingKey()).isEqualTo("receivedRoutingKey");
		assertThat(properties.getRedelivered()).isTrue();
		assertThat(properties.getTimestamp().getTime() > 0).isTrue();
		assertThat(properties.getType()).isEqualTo("type");
		assertThat(properties.getUserId()).isEqualTo("userId");
	}

	private void assertUpper(MessageProperties properties) {
		assertThat(properties.getAppId()).isEqualTo("APPID");
		assertThat(properties.getClusterId()).isEqualTo("CLUSTERID");
		assertThat(properties.getContentEncoding()).isEqualTo("CONTENTENCODING");
		assertThat(properties.getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_BYTES);
		assertThat(properties.getContentLength()).isEqualTo(10);
		assertThat(properties.getCorrelationId()).isEqualTo("CORRELATIONID");
		assertThat(properties.getDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
		assertThat(properties.getDeliveryTag()).isEqualTo(20);
		assertThat(properties.getExpiration()).isEqualTo("EXPIRATION");
		assertThat(properties.getHeaders().get("foo")).isEqualTo("BAR");
		assertThat(properties.getHeaders().get("qux")).isEqualTo("FIZ");
		assertThat(properties.getHeaders().get("baz")).isEqualTo("FUZ");
		assertThat(properties.getMessageCount()).isEqualTo(Integer.valueOf(30));
		assertThat(properties.getMessageId()).isEqualTo("MESSAGEID");
		assertThat(properties.getPriority()).isEqualTo(Integer.valueOf(40));
		assertThat(properties.getReceivedExchange()).isEqualTo("RECEIVEDEXCHANGE");
		assertThat(properties.getReceivedRoutingKey()).isEqualTo("RECEIVEDROUTINGKEY");
		assertThat(properties.getRedelivered()).isFalse();
		assertThat(properties.getTimestamp().getTime() == 0).isTrue();
		assertThat(properties.getType()).isEqualTo("TYPE");
		assertThat(properties.getUserId()).isEqualTo("USERID");
	}

}
