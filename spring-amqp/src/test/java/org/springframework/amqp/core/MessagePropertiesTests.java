/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

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

		properties.setDeliveryTime((Instant) null);
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
	public void tesNoNullPointerInEquals() {
		MessageProperties mp = new MessageProperties();
		MessageProperties mp2 = new MessageProperties();
		assertThat(mp.equals(mp2)).isTrue();
	}

	 @Test
		public void tesNoNullPointerInHashCode() {
			Set<MessageProperties> messageList = new HashSet<>();
			messageList.add(new MessageProperties());
			assertThat(messageList).hasSize(1);
		}

}
