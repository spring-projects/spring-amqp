/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author Dave Syer
 * @author Artem Yakshin
 * @author Artem Bilan
 * @author Gary Russell
 * @author Csaba Soti
 * @author Raylax Grey
 * @author Ngoc Nhan
 *
 */
public class MessagePropertiesTests {

	@Test
	public void testReplyTo() {
		MessageProperties properties = new MessageProperties();
		properties.setReplyTo("foo/bar");
		assertThat(properties.getReplyToAddress().getRoutingKey()).isEqualTo("bar");
	}

	@Test
	public void testReplyToNullByDefault() {
		MessageProperties properties = new MessageProperties();
		assertThat(properties.getReplyTo()).isEqualTo(null);
		assertThat(properties.getReplyToAddress()).isEqualTo(null);
	}

	@Test
	public void testNativeDeliveryTimeHeader() {
		MessageProperties properties = new MessageProperties();
		Instant deliveryTime = Instant.ofEpochMilli(1_800_000_000_123L);

		properties.setDeliveryTime(deliveryTime);

		assertThat(properties.getHeaders().get(MessageProperties.DELIVERY_TIME))
				.isEqualTo(deliveryTime.toEpochMilli());
		assertThat(properties.getDeliveryTime()).isEqualTo(deliveryTime);
		assertThat(properties.getDeliveryTimeEpochMillis()).isEqualTo(deliveryTime.toEpochMilli());

		properties.setDeliveryTime(1_800_000_000_456L);
		assertThat(properties.getDeliveryTime()).isEqualTo(Instant.ofEpochMilli(1_800_000_000_456L));

		properties.setDeliveryTime(null);
		assertThat(properties.getHeaders()).doesNotContainKey(MessageProperties.DELIVERY_TIME);
		assertThat(properties.getDeliveryTime()).isNull();
	}

	@Test
	public void testContentLengthSet() {
		MessageProperties properties = new MessageProperties();
		properties.setContentLength(1L);
		assertThat(properties.isContentLengthSet()).isTrue();
	}

	@Test
	public void testNoNullPointerInEquals() {
		MessageProperties mp = new MessageProperties();
		MessageProperties mp2 = new MessageProperties();
		assertThat(mp.equals(mp2)).isTrue();
	}

	@Test
	public void testNoNullPointerInHashCode() {
		Set<MessageProperties> messageList = new HashSet<>();
		messageList.add(new MessageProperties());
		assertThat(messageList).hasSize(1);
	}

	@Test
	public void equalsAndHashCodeCompareTheSameProperties() {

		Map<String, Consumer<MessageProperties>> changes = Map.ofEntries(
			Map.entry("appId", properties -> properties.setAppId("application")),
			Map.entry("clusterId", properties -> properties.setClusterId("cluster")),
			Map.entry("contentEncoding", properties -> properties.setContentEncoding("utf-8")),
			Map.entry("contentLength", properties -> properties.setContentLength(1)),
			Map.entry("contentType", properties -> properties.setContentType("text/plain")),
			Map.entry("correlationId", properties -> properties.setCorrelationId("correlation")),
			Map.entry("deliveryMode", properties -> properties.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT)),
			Map.entry("deliveryTag", properties -> properties.setDeliveryTag(1)),
			Map.entry("expiration", properties -> properties.setExpiration("1000")),
			Map.entry("headers", properties -> properties.setHeader("header", "value")),
			Map.entry("messageCount", properties -> properties.setMessageCount(1)),
			Map.entry("messageId", properties -> properties.setMessageId("message")),
			Map.entry("priority", properties -> properties.setPriority(1)),
			Map.entry("receivedExchange", properties -> properties.setReceivedExchange("exchange")),
			Map.entry("receivedRoutingKey", properties -> properties.setReceivedRoutingKey("routing-key")),
			Map.entry("redelivered", properties -> properties.setRedelivered(true)),
			Map.entry("replyTo", properties -> properties.setReplyTo("reply")),
			Map.entry("timestamp", properties -> properties.setTimestamp(new Date(1))),
			Map.entry("type", properties -> properties.setType("type")),
			Map.entry("userId", properties -> properties.setUserId("user"))
		);

		changes.forEach((field, change) -> {

			MessageProperties original = new MessageProperties();
			MessageProperties changed = new MessageProperties();

			change.accept(original);
			change.accept(changed);

			assertThat(original.equals(changed)).as(field).isTrue();
			assertThat(changed.equals(original)).as(field).isTrue();
			assertThat(original.hashCode()).as(field).isEqualTo(changed.hashCode());
		});
	}

}
