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

package org.springframework.amqp.support.converter;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.core.NestedIOException;
import org.springframework.core.serializer.DefaultDeserializer;
import org.springframework.core.serializer.Deserializer;

/**
 * @author Mark Fisher
 * @author Gary Russell
 */
public class SerializerMessageConverterTests extends WhiteListDeserializingMessageConverterTests {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void bytesAsDefaultMessageBodyType() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		Message message = new Message("test".getBytes(), new MessageProperties());
		Object result = converter.fromMessage(message);
		assertEquals(byte[].class, result.getClass());
		assertEquals("test", new String((byte[]) result, "UTF-8"));
	}

	@Test
	public void messageToString() {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		Message message = new Message("test".getBytes(), new MessageProperties());
		message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		Object result = converter.fromMessage(message);
		assertEquals(String.class, result.getClass());
		assertEquals("test", result);
	}

	@Test
	public void messageToBytes() {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		Message message = new Message(new byte[] { 1, 2, 3 }, new MessageProperties());
		message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_BYTES);
		Object result = converter.fromMessage(message);
		assertEquals(byte[].class, result.getClass());
		byte[] resultBytes = (byte[]) result;
		assertEquals(3, resultBytes.length);
		assertEquals(1, resultBytes[0]);
		assertEquals(2, resultBytes[1]);
		assertEquals(3, resultBytes[2]);
	}

	@Test
	public void messageToSerializedObject() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		MessageProperties properties = new MessageProperties();
		properties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
		TestBean testBean = new TestBean("foo");
		objectStream.writeObject(testBean);
		objectStream.flush();
		objectStream.close();
		byte[] bytes = byteStream.toByteArray();
		Message message = new Message(bytes, properties);
		Object result = converter.fromMessage(message);
		assertEquals(TestBean.class, result.getClass());
		assertEquals(testBean, result);
	}

	@Test
	public void messageToSerializedObjectNoContentType() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		converter.setIgnoreContentType(true);
		MessageProperties properties = new MessageProperties();
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
		TestBean testBean = new TestBean("foo");
		objectStream.writeObject(testBean);
		objectStream.flush();
		objectStream.close();
		byte[] bytes = byteStream.toByteArray();
		Message message = new Message(bytes, properties);
		Object result = converter.fromMessage(message);
		assertEquals(TestBean.class, result.getClass());
		assertEquals(testBean, result);
	}

	@Test
	public void stringToMessage() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		Message message = converter.toMessage("test", new MessageProperties());
		String contentType = message.getMessageProperties().getContentType();
		String content = new String(message.getBody(),
				message.getMessageProperties().getContentEncoding());
		assertEquals("text/plain", contentType);
		assertEquals("test", content);
	}

	@Test
	public void bytesToMessage() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		Message message = converter.toMessage(new byte[] { 1, 2, 3 }, new MessageProperties());
		String contentType = message.getMessageProperties().getContentType();
		byte[] body = message.getBody();
		assertEquals("application/octet-stream", contentType);
		assertEquals(3, body.length);
		assertEquals(1, body[0]);
		assertEquals(2, body[1]);
		assertEquals(3, body[2]);
	}

	@Test
	public void serializedObjectToMessage() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		TestBean testBean = new TestBean("foo");
		Message message = converter.toMessage(testBean, new MessageProperties());
		String contentType = message.getMessageProperties().getContentType();
		byte[] body = message.getBody();
		assertEquals("application/x-java-serialized-object", contentType);
		ByteArrayInputStream bais = new ByteArrayInputStream(body);
		Object deserializedObject = new ObjectInputStream(bais).readObject();
		assertEquals(testBean, deserializedObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDefaultDeserializerClassLoader() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		ClassLoader loader = mock(ClassLoader.class);
		Deserializer<Object> deserializer = new DefaultDeserializer(loader);
		converter.setDeserializer(deserializer);
		assertSame(loader, TestUtils.getPropertyValue(converter, "defaultDeserializerClassLoader"));
		assertTrue(TestUtils.getPropertyValue(converter, "usingDefaultDeserializer", Boolean.class));
		Deserializer<Object> mock = mock(Deserializer.class);
		converter.setDeserializer(mock);
		assertFalse(TestUtils.getPropertyValue(converter, "usingDefaultDeserializer", Boolean.class));
		TestBean testBean = new TestBean("foo");
		Message message = converter.toMessage(testBean, new MessageProperties());
		converter.fromMessage(message);
		verify(mock).deserialize(Mockito.any(InputStream.class));
	}

	@Test
	public void messageConversionExceptionForClassNotFound() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		TestBean testBean = new TestBean("foo");
		Message message = converter.toMessage(testBean, new MessageProperties());
		String contentType = message.getMessageProperties().getContentType();
		assertEquals("application/x-java-serialized-object", contentType);
		byte[] body = message.getBody();
		body[10] = 'z';
		this.exception.expect(MessageConversionException.class);
		this.exception.expectCause(instanceOf(NestedIOException.class));
		converter.fromMessage(message);
	}

}
