/*
 * Copyright 2002-2012 the original author or authors.
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
package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;

/**
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class RabbitTemplateTests {

	@Test
	public void testConvertbytes() {
		RabbitTemplate template = new RabbitTemplate();
		byte[] payload = "Hello, world!".getBytes();
		Message message = template.convertMessageIfNecessary(payload);
		assertSame(payload, message.getBody());
	}

	@Test
	public void testConvertString() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		String payload = "Hello, world!";
		Message message = template.convertMessageIfNecessary(payload);
		assertEquals(payload, new String(message.getBody(), SimpleMessageConverter.DEFAULT_CHARSET));
	}

	@Test
	public void testConvertSerializable() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		Long payload = 43L;
		Message message = template.convertMessageIfNecessary(payload);
		assertEquals(payload, SerializationUtils.deserialize(message.getBody()));
	}

	@Test
	public void testConvertMessage() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		Message input = new Message("Hello, world!".getBytes(), new MessageProperties());
		Message message = template.convertMessageIfNecessary(input);
		assertSame(input, message);
	}

}
