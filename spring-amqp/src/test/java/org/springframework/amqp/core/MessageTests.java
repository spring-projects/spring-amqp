/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 */
public class MessageTests {

	@Test
	public void toStringForEmptyMessageBody() {
		Message message = new Message(new byte[0], new MessageProperties());
		assertThat(message.toString()).isNotNull();
	}

	@Test
	public void properEncoding() {
		Message message = new Message("ÁRVÍZTŰRŐ TÜKÖRFÚRÓGÉP".getBytes(StandardCharsets.UTF_16),
				new MessageProperties());
		message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_JSON);
		message.getMessageProperties().setContentEncoding("UTF-16");
		assertThat(message.toString()).contains("ÁRVÍZTŰRŐ TÜKÖRFÚRÓGÉP");
	}

	@Test
	public void toStringForNullMessageProperties() {
		Message message = new Message(new byte[0]);
		assertThat(message.toString()).isNotNull();
	}

	@Test
	public void toStringForNonStringMessageBody() {
		Message message = new Message(SerializationUtils.serialize(new Date()));
		assertThat(message.toString()).isNotNull();
	}

	@Test
	public void toStringForSerializableMessageBody() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		Message message = new Message(SerializationUtils.serialize(new Date()), messageProperties);
		assertThat(message.toString()).isNotNull();
	}

	@Test
	public void toStringForNonSerializableMessageBody() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		Message message = new Message("foo".getBytes(), messageProperties);
		assertThat(message.toString()).isNotNull();
	}

	@Test
	public void serialization() throws Exception {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		messageProperties.setHeader("foo", "bar");
		messageProperties.setContentType("text/plain");
		Message message = new Message("baz".getBytes(), messageProperties);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(baos);
		os.writeObject(message);
		os.close();
		ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
		Message out = (Message) is.readObject();
		assertThat(new String(out.getBody())).isEqualTo(new String(message.getBody()));
		assertThat(out.toString()).isEqualTo(message.toString());
	}

	@Test
	public void fooNotDeserialized() {
		Message message = new SimpleMessageConverter().toMessage(new Foo(), new MessageProperties());
		assertThat(message.toString()).doesNotContainPattern("aFoo");
		Message listMessage = new SimpleMessageConverter().toMessage(Collections.singletonList(new Foo()),
				new MessageProperties());
		assertThat(listMessage.toString()).doesNotContainPattern("aFoo");
		assertThat(message.toString()).contains("[serialized object]");
		assertThat(listMessage.toString()).contains("[serialized object]");
	}

	@Test
	void dontToStringLongBody() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		StringBuilder builder1 = new StringBuilder();
		IntStream.range(0, 50).forEach(i -> builder1.append("x"));
		String bodyAsString = builder1.toString();
		Message message = new Message(bodyAsString.getBytes(), messageProperties);
		assertThat(message.toString()).contains(bodyAsString);
		StringBuilder builder2 = new StringBuilder();
		IntStream.range(0, 51).forEach(i -> builder2.append("x"));
		bodyAsString = builder2.toString();
		message = new Message(bodyAsString.getBytes(), messageProperties);
		assertThat(message.toString()).contains("[51]");
		Message.setMaxBodyLength(100);
		assertThat(message.toString()).contains(bodyAsString);
	}

	@SuppressWarnings("serial")
	public static class Foo implements Serializable {

		@Override
		public String toString() {
			return "aFoo";
		}

	}

}
