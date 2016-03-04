/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.MessageProperties;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;

/**
 * @author Soeren Unruh
 * @author Gary Russell
 * @since 1.3
 */
public class DefaultMessagePropertiesConverterTests {

	private final DefaultMessagePropertiesConverter messagePropertiesConverter =
			new DefaultMessagePropertiesConverter();

	private final Envelope envelope = new Envelope(0, false, null, null);

	private final LongString longString = LongStringHelper.asLongString("longString");

	private String longStringString;

	@Before
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
		assertEquals("LongString not converted to String",
				longStringString, messageProperties.getHeaders().get("longString"));
	}

	@Test
	public void testToMessagePropertiesLongStringInList() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("list", Arrays.asList(longString));
		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();
		MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(source, envelope, "UTF-8");
		assertEquals("LongString nested in List not converted to String",
				longStringString, ((List<?>) messageProperties.getHeaders().get("list")).get(0));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testToMessagePropertiesLongStringDeepInList() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("list", Arrays.asList(Arrays.asList(longString)));
		BasicProperties source = new BasicProperties.Builder()
				.headers(headers)
				.build();
		MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(source, envelope, "UTF-8");
		assertEquals("LongString deeply nested in List not converted to String",
				longStringString, ((List<?>) ((List<?>) messageProperties.getHeaders().get("list")).get(0)).get(0));
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
		assertEquals("LongString nested in Map not converted to String",
				longStringString, ((Map<String, Object>) messageProperties.getHeaders().get("map")).get("longString"));
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
		assertThat(messageProperties.getHeaders().get("longString"), instanceOf(String.class));
		assertThat(messageProperties.getHeaders().get("string1025"), instanceOf(DataInputStream.class));
		assertThat(messageProperties.getHeaders().get("string1026"), instanceOf(DataInputStream.class));
		MessagePropertiesConverter longConverter = new DefaultMessagePropertiesConverter(1025, true);
		messageProperties = longConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(messageProperties.getHeaders().get("longString"), instanceOf(String.class));
		assertThat(messageProperties.getHeaders().get("string1025"), instanceOf(String.class));
		assertThat(messageProperties.getHeaders().get("string1026"), instanceOf(DataInputStream.class));

		longConverter = new DefaultMessagePropertiesConverter(1025);
		messageProperties = longConverter.toMessageProperties(source, envelope, "UTF-8");
		assertThat(messageProperties.getHeaders().get("longString"), instanceOf(String.class));
		assertThat(messageProperties.getHeaders().get("string1025"), instanceOf(String.class));
		assertThat(messageProperties.getHeaders().get("string1026"), instanceOf(LongString.class));

		BasicProperties basicProperties = longConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertEquals(longString1026.toString(), basicProperties.getHeaders().get("string1026").toString());
	}

	@Test
	public void testFromUnsupportedValue() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setHeader("unsupported", new Object());
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertTrue("Unsupported value not converted to String",
				basicProps.getHeaders().get("unsupported") instanceof String);
	}

	@Test
	public void testFromUnsupportedValueInList() {
		MessageProperties messageProperties = new MessageProperties();
		List<Object> listWithUnsupportedValue = Arrays.asList(new Object());
		messageProperties.setHeader("list", listWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertTrue("Unsupported value nested in List not converted to String",
				((List<?>) basicProps.getHeaders().get("list")).get(0) instanceof String);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFromUnsupportedValueDeepInList() {
		MessageProperties messageProperties = new MessageProperties();
		List<List<Object>> listWithUnsupportedValue = Arrays.asList(Arrays.asList(new Object()));
		messageProperties.setHeader("list", listWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertTrue("Unsupported value deeply nested in List not converted to String",
				((List<Object>) ((List<?>) basicProps.getHeaders().get("list")).get(0)).get(0) instanceof String);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFromUnsupportedValueInMap() {
		MessageProperties messageProperties = new MessageProperties();
		Map<String, Object> mapWithUnsupportedValue = new HashMap<String, Object>();
		mapWithUnsupportedValue.put("unsupported", new Object());
		messageProperties.setHeader("map", mapWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertTrue("Unsupported value nested in Map not converted to String",
				((Map<String, Object>) basicProps.getHeaders().get("map")).get("unsupported") instanceof String);
	}

	@Test
	public void testCorrelationIdAsString() {
		MessageProperties messageProperties = new MessageProperties();
		this.messagePropertiesConverter
				.setCorrelationIdAsString(DefaultMessagePropertiesConverter.CorrelationIdPolicy.BOTH);
		messageProperties.setCorrelationIdString("foo");
		messageProperties.setCorrelationId("bar".getBytes()); // foo should win
		BasicProperties basicProps = this.messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertEquals("foo", basicProps.getCorrelationId());
		messageProperties = this.messagePropertiesConverter.toMessageProperties(basicProps, null, "UTF-8");
		assertEquals("foo", messageProperties.getCorrelationIdString());
		assertEquals("foo", new String(messageProperties.getCorrelationId()));

		this.messagePropertiesConverter
				.setCorrelationIdAsString(DefaultMessagePropertiesConverter.CorrelationIdPolicy.STRING);
		messageProperties.setCorrelationIdString("foo");
		messageProperties.setCorrelationId("bar".getBytes()); // foo should win
		basicProps = this.messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertEquals("foo", basicProps.getCorrelationId());
		messageProperties = this.messagePropertiesConverter.toMessageProperties(basicProps, null, "UTF-8");
		assertEquals("foo", messageProperties.getCorrelationIdString());
		assertNull(messageProperties.getCorrelationId());

		this.messagePropertiesConverter
				.setCorrelationIdAsString(DefaultMessagePropertiesConverter.CorrelationIdPolicy.BYTES);
		messageProperties.setCorrelationIdString("foo");
		messageProperties.setCorrelationId("bar".getBytes()); // bar should win
		basicProps = this.messagePropertiesConverter.fromMessageProperties(messageProperties, "UTF-8");
		assertEquals("bar", basicProps.getCorrelationId());
		messageProperties = this.messagePropertiesConverter.toMessageProperties(basicProps, null, "UTF-8");
		assertNull(messageProperties.getCorrelationIdString());
		assertEquals("bar", new String(messageProperties.getCorrelationId()));
	}

}
