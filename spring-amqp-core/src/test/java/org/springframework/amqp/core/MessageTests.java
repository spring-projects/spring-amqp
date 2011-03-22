/*
 * Copyright 2002-2010 the original author or authors.
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

import static org.junit.Assert.assertNotNull;

import java.util.Date;

import org.junit.Test;
import org.springframework.amqp.utils.SerializationUtils;

/**
 * @author Mark Fisher
 * @author Dave Syer
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
		// System.err.println(message);
		assertNotNull(message.toString());
	}

}
