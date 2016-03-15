/*
 * Copyright 2016 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Collections;

import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;

/**
 * @author Gary Russell
 * @since 1.5.5
 *
 */
public class WhiteListDeserializingMessageConverterTests {

	@Test
	public void testWhiteList() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		TestBean testBean = new TestBean("foo");
		Message message = converter.toMessage(testBean, new MessageProperties());
		Object fromMessage = converter.fromMessage(message);
		assertEquals(testBean, fromMessage);

		converter.setWhiteListPatterns(Collections.singletonList("*"));
		fromMessage = converter.fromMessage(message);
		assertEquals(testBean, fromMessage);

		converter.setWhiteListPatterns(Collections.singletonList("org.springframework.amqp.*"));
		fromMessage = converter.fromMessage(message);
		assertEquals(testBean, fromMessage);
		converter.setWhiteListPatterns(Collections.singletonList("*$TestBean"));
		fromMessage = converter.fromMessage(message);
		assertEquals(testBean, fromMessage);

		try {
			converter.setWhiteListPatterns(Collections.singletonList("foo.*"));
			fromMessage = converter.fromMessage(message);
			assertEquals(testBean, fromMessage);
			fail("Expected SecurityException");
		}
		catch (SecurityException e) {

		}
	}

	@SuppressWarnings("serial")
	protected static class TestBean implements Serializable {

		private final String text;

		protected TestBean(String text) {
			Assert.notNull(text, "text must not be null");
			this.text = text;
		}

		@Override
		public boolean equals(Object other) {
			return (other instanceof TestBean && this.text.equals(((TestBean) other).text));
		}

		@Override
		public int hashCode() {
			return this.text.hashCode();
		}
	}


}
