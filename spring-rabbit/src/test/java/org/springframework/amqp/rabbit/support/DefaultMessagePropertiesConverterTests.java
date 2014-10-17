/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.support;

import static org.junit.Assert.*;

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
 *
 */
public class DefaultMessagePropertiesConverterTests {
	private final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();
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
	
	@SuppressWarnings("unchecked")
	@Test
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
	
	@SuppressWarnings("unchecked")
	@Test
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
	public void testFromUnsupportedValue() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setHeader("unsupported", new Object());
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties , "UTF-8");
		assertTrue("Unsupported value not converted to String",
				basicProps.getHeaders().get("unsupported") instanceof String);
	}
	@Test
	public void testFromUnsupportedValueInList() {
		MessageProperties messageProperties = new MessageProperties();
		List<Object> listWithUnsupportedValue = Arrays.asList(new Object());
		messageProperties.setHeader("list", listWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties , "UTF-8");
		assertTrue("Unsupported value nested in List not converted to String",
				((List<?>)basicProps.getHeaders().get("list")).get(0) instanceof String);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFromUnsupportedValueDeepInList() {
		MessageProperties messageProperties = new MessageProperties();
		List<List<Object>> listWithUnsupportedValue = Arrays.asList(Arrays.asList(new Object()));
		messageProperties.setHeader("list", listWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties , "UTF-8");
		assertTrue("Unsupported value deeply nested in List not converted to String",
				((List<Object>)((List<?>)basicProps.getHeaders().get("list")).get(0)).get(0) instanceof String);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFromUnsupportedValueInMap() {
		MessageProperties messageProperties = new MessageProperties();
		Map<String, Object> mapWithUnsupportedValue = new HashMap<String, Object>();
		mapWithUnsupportedValue.put("unsupported", new Object());
		messageProperties.setHeader("map", mapWithUnsupportedValue);
		BasicProperties basicProps = messagePropertiesConverter.fromMessageProperties(messageProperties , "UTF-8");
		assertTrue("Unsupported value nested in Map not converted to String",
				((Map<String, Object>)basicProps.getHeaders().get("map")).get("unsupported") instanceof String);
	}
}
