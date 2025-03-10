/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.io.DataInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soeren Unruh
 * @author Gary Russell
 * @author Johan Kaving
 * @since 1.3
 */
public class DefaultMessagePropertiesConverterTests {

	private final DefaultMessagePropertiesConverter messagePropertiesConverter =
			new DefaultMessagePropertiesConverter();

	private final Envelope envelope = new Envelope(0, false, null, null);

	private final LongString longString = LongStringHelper.asLongString("longString");

	private String longStringString;

	@BeforeEach
	public void init() throws UnsupportedEncodingException {
		longStringString = new String(longString.getBytes(), "UTF-8");
	}

	@Test
	public void testToMessagePropertiesLongString() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("longString", longString);
		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();
		MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(messageProperties.getHeaders().get("longString")).as("LongString not converted to String").isEqualTo(longStringString);
	}

	@Test
	public void testToMessagePropertiesLongStringInList() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("list", Arrays.asList(longString));
		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();
		MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(((List<?>) messageProperties.getHeaders().get("list")).get(0)).as("LongString nested in List not converted to String").isEqualTo(longStringString);
	}

	@Test
	public void testToMessagePropertiesLongStringDeepInList() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("list", Arrays.asList(Arrays.asList(longString)));
		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();
		MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(((List<?>) ((List<?>) messageProperties.getHeaders().get("list")).get(0)).get(0)).as("LongString deeply nested in List not converted to String").isEqualTo(longStringString);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testToMessagePropertiesLongStringInMap() {
		Map<String, Object> mapWithLongString = new HashMap<String, Object>();
		mapWithLongString.put("longString", longString);
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("map", mapWithLongString);
		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();
		MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(((Map<String, Object>) messageProperties.getHeaders().get("map")).get("longString")).as("LongString nested in Map not converted to String").isEqualTo(longStringString);
	}

	@Test
	public void testToMessagePropertiesXDeathCount() {
		Map<String, Object> headers = new HashMap<String, Object>();

		headers.put("x-death", List.of(Map.of("count", Integer.valueOf(2))));

		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();

		MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(source, envelope, "UTF-8");

		assertThat(messageProperties.getRetryCount()).isEqualTo(2);
	}

	@Test
	public void testLongLongString() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("longString", longString);
		headers.put("string1025", LongStringHelper.asLongString(new byte[1025]));
		byte[] longBytes = new byte[1026];
		longBytes[0] = 'a';
		longBytes[1025] = 'z';
		LongString longString1026 = LongStringHelper.asLongString(longBytes);
		headers.put("string1026", longString1026);
		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();
		MessagePropertiesConverter converter = new DefaultMessagePropertiesConverter(1024, true);
		MessageProperties messageProperties = converter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(messageProperties.getHeaders().get("longString")).isInstanceOf(String.class);
		assertThat(messageProperties.getHeaders().get("string1025")).isInstanceOf(DataInputStream.class);
		assertThat(messageProperties.getHeaders().get("string1026")).isInstanceOf(DataInputStream.class);
		MessagePropertiesConverter longConverter = new DefaultMessagePropertiesConverter(1025, true);
		messageProperties = longConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(messageProperties.getHeaders().get("longString")).isInstanceOf(String.class);
		assertThat(messageProperties.getHeaders().get("string1025")).isInstanceOf(String.class);
		assertThat(messageProperties.getHeaders().get("string1026")).isInstanceOf(DataInputStream.class);

		longConverter = new DefaultMessagePropertiesConverter(1025);
		messageProperties = longConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(messageProperties.getHeaders().get("longString")).isInstanceOf(String.class);
		assertThat(messageProperties.getHeaders().get("string1025")).isInstanceOf(String.class);
		assertThat(messageProperties.getHeaders().get("string1026")).isInstanceOf(LongString.class);

		BasicProperties basicProperties = longConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertThat(basicProperties.getHeaders().get("string1026").toString()).isEqualTo(longString1026.toString());
	}

	@Test
	public void testFromUnsupportedValue() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setHeader("unsupported", new Object());
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertThat(basicProps.getHeaders().get("unsupported") instanceof String).as("Unsupported value not converted to String").isTrue();
	}

	@Test
	public void testFromUnsupportedValueInList() {
		MessageProperties messageProperties = new MessageProperties();
		List<Object> listWithUnsupportedValue = Arrays.asList(new Object());
		messageProperties.setHeader("list", listWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertThat(((List<?>) basicProps.getHeaders().get("list")).get(0) instanceof String).as("Unsupported value nested in List not converted to String").isTrue();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFromUnsupportedValueDeepInList() {
		MessageProperties messageProperties = new MessageProperties();
		List<List<Object>> listWithUnsupportedValue = Arrays.asList(Arrays.asList(new Object()));
		messageProperties.setHeader("list", listWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertThat(((List<Object>) ((List<?>) basicProps.getHeaders().get("list")).get(0)).get(0) instanceof String).as("Unsupported value deeply nested in List not converted to String").isTrue();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFromUnsupportedValueInMap() {
		MessageProperties messageProperties = new MessageProperties();
		Map<String, Object> mapWithUnsupportedValue = new HashMap<String, Object>();
		mapWithUnsupportedValue.put("unsupported", new Object());
		messageProperties.setHeader("map", mapWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertThat(((Map<String, Object>) basicProps.getHeaders().get("map")).get("unsupported") instanceof String).as("Unsupported value nested in Map not converted to String").isTrue();
	}

	@Test
	public void testInboundDeliveryMode() {
		DefaultMessagePropertiesConverter converter = new DefaultMessagePropertiesConverter();
		MessageProperties props = new MessageProperties();
		String[] strings = new String[] { "1", "2" };
		props.getHeaders().put("strings", strings);
		props.getHeaders().put("objects", new Object[] { new Foo() });
		props.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		BasicProperties bProps = converter.fromMessageProperties(props, "UTF-8");
		assertThat(bProps.getDeliveryMode().intValue()).isEqualTo(MessageDeliveryMode.toInt(MessageDeliveryMode.NON_PERSISTENT));
		props = converter.toMessageProperties(bProps, null, "UTF-8");
		assertThat(props.getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat((Object[]) props.getHeaders().get("strings")).isEqualTo(strings);
		assertThat(Arrays.asList((Object[]) props.getHeaders().get("objects")).toString()).isEqualTo("[FooAsAString]");
		assertThat(props.getDeliveryMode()).isNull();
	}

	@Test
	public void testClassHeader() {
		MessageProperties props = new MessageProperties();
		props.setHeader("aClass", getClass());
		BasicProperties basic = new DefaultMessagePropertiesConverter().fromMessageProperties(props, "UTF8");
		assertThat(basic.getHeaders().get("aClass")).isEqualTo(getClass().getName());
	}

	@Test
	public void testRetryCount() {
		MessageProperties props = new MessageProperties();
		props.incrementRetryCount();
		BasicProperties basic = new DefaultMessagePropertiesConverter().fromMessageProperties(props, "UTF8");
		assertThat(basic.getHeaders().get(MessageProperties.RETRY_COUNT)).isEqualTo(1L);
		props.incrementRetryCount();
		basic = new DefaultMessagePropertiesConverter().fromMessageProperties(props, "UTF8");
		assertThat(basic.getHeaders().get(MessageProperties.RETRY_COUNT)).isEqualTo(2L);
	}

	private static class Foo {

		Foo() {
		}

		@Override
		public String toString() {
			return "FooAsAString";
		}

	}

}
