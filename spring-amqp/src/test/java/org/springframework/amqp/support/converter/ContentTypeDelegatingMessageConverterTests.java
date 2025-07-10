/*
 * Copyright 2015-present the original author or authors.
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

import java.io.Serializable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4.2
 *
 */
public class ContentTypeDelegatingMessageConverterTests {

	@BeforeAll
	static void setUp() {
		System.setProperty("spring.amqp.deserialization.trust.all", "true");
	}

	@AfterAll
	static void tearDown() {
		System.setProperty("spring.amqp.deserialization.trust.all", "false");
	}

	@Test
	public void testDelegationOutbound() {
		ContentTypeDelegatingMessageConverter converter = new ContentTypeDelegatingMessageConverter();
		JacksonJsonMessageConverter messageConverter =
				new JacksonJsonMessageConverter(ContentTypeDelegatingMessageConverterTests.class.getPackage().getName());
		converter.addDelegate("foo/bar", messageConverter);
		converter.addDelegate(MessageProperties.CONTENT_TYPE_JSON, messageConverter);
		MessageProperties props = new MessageProperties();
		Foo foo = new Foo();
		foo.setFoo("bar");
		Message message = converter.toMessage(foo, props);
		assertThat(message.getMessageProperties().getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		Object converted = converter.fromMessage(message);
		assertThat(converted).isInstanceOf(Foo.class);

		props.setContentType("foo/bar");
		message = converter.toMessage(foo, props);
		assertThat(message.getMessageProperties().getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_JSON);
		assertThat(new String(message.getBody())).isEqualTo("{\"foo\":\"bar\"}");
		converted = converter.fromMessage(message);
		assertThat(converted).isInstanceOf(Foo.class);
	}

	@SuppressWarnings("serial")
	public static class Foo implements Serializable {

		private String foo;

		public String getFoo() {
			return foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

	}

}
