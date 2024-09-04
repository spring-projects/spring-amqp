/*
 * Copyright 2016-2024 the original author or authors.
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

package org.springframework.amqp.support.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.Serializable;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;

/**
 * @author Gary Russell
 * @since 1.5.5
 *
 */
public class AllowedListDeserializingMessageConverterTests {

	@Test
	public void testAllowedList() throws Exception {
		SerializerMessageConverter converter = new SerializerMessageConverter();
		TestBean testBean = new TestBean("foo");
		Message message = converter.toMessage(testBean, new MessageProperties());
		// when env var not set
//		assertThatExceptionOfType(SecurityException.class).isThrownBy(() -> converter.fromMessage(message));
		Object fromMessage;
		// when env var set.
		fromMessage = converter.fromMessage(message);
		assertThat(fromMessage).isEqualTo(testBean);

		converter.setAllowedListPatterns(Collections.singletonList("*"));
		fromMessage = converter.fromMessage(message);
		assertThat(fromMessage).isEqualTo(testBean);

		converter.setAllowedListPatterns(Collections.singletonList("org.springframework.amqp.*"));
		fromMessage = converter.fromMessage(message);
		assertThat(fromMessage).isEqualTo(testBean);
		converter.setAllowedListPatterns(Collections.singletonList("*$TestBean"));
		fromMessage = converter.fromMessage(message);
		assertThat(fromMessage).isEqualTo(testBean);

		converter.setAllowedListPatterns(Collections.singletonList("foo.*"));
		assertThatExceptionOfType(SecurityException.class).isThrownBy(() -> converter.fromMessage(message));
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
			return (other instanceof TestBean testBean && this.text.equals(testBean.text));
		}

		@Override
		public int hashCode() {
			return this.text.hashCode();
		}
	}


}
