/*
 * Copyright 2002-2013 the original author or authors.
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

package org.springframework.amqp.core;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Date;

import org.junit.Test;

import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;

/**
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 */
public class MessageTests {

	@Test
	public void toStringForEmptyMessageBody() {
		Message message = new Message(new byte[0], new MessageProperties());
		assertNotNull(message.toString());
	}

	@Test
	public void toStringForNullMessageProperties() {
		Message message = new Message(new byte[0], null);
		assertNotNull(message.toString());
	}

	@Test
	public void toStringForNonStringMessageBody() {
		Message message = new Message(SerializationUtils.serialize(new Date()), null);
		assertNotNull(message.toString());
	}

	@Test
	public void toStringForSerializableMessageBody() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		Message message = new Message(SerializationUtils.serialize(new Date()), messageProperties);
		assertNotNull(message.toString());
	}

	@Test
	public void toStringForNonSerializableMessageBody() {
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		Message message = new Message("foo".getBytes(), messageProperties);
		assertNotNull(message.toString());
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
		assertEquals(new String(message.getBody()), new String(out.getBody()));
		assertEquals(message.toString(), out.toString());
	}

	@Test
	public void fooNotDeserialized() {
		Message message = new SimpleMessageConverter().toMessage(new Foo(), new MessageProperties());
		assertThat(message.toString(), not(containsString("aFoo")));
		Message listMessage = new SimpleMessageConverter().toMessage(Collections.singletonList(new Foo()),
				new MessageProperties());
		assertThat(listMessage.toString(), not(containsString("aFoo")));
		Message.addWhiteListPatterns(Foo.class.getName());
		assertThat(message.toString(), containsString("aFoo"));
		assertThat(listMessage.toString(), containsString("aFoo"));
	}

	@SuppressWarnings("serial")
	public static class Foo implements Serializable {

		@Override
		public String toString() {
			return "aFoo";
		}

	}

}
