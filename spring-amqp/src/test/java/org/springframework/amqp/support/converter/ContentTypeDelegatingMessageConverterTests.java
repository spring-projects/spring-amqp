/*
 * Copyright 2015-2017 the original author or authors.
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.Serializable;

import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4.2
 *
 */
public class ContentTypeDelegatingMessageConverterTests {

	@Test
	public void testDelegationOutbound() {
		ContentTypeDelegatingMessageConverter converter = new ContentTypeDelegatingMessageConverter();
		Jackson2JsonMessageConverter messageConverter =
				new Jackson2JsonMessageConverter(ContentTypeDelegatingMessageConverterTests.class.getPackage().getName());
		converter.addDelegate("foo/bar", messageConverter);
		converter.addDelegate(MessageProperties.CONTENT_TYPE_JSON, messageConverter);
		MessageProperties props = new MessageProperties();
		Foo foo = new Foo();
		foo.setFoo("bar");
		Message message = converter.toMessage(foo, props);
		assertEquals(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT, message.getMessageProperties().getContentType());
		Object converted = converter.fromMessage(message);
		assertThat(converted, instanceOf(Foo.class));

		props.setContentType("foo/bar");
		message = converter.toMessage(foo, props);
		assertEquals(MessageProperties.CONTENT_TYPE_JSON, message.getMessageProperties().getContentType());
		assertEquals("{\"foo\":\"bar\"}", new String(message.getBody()));
		converted = converter.fromMessage(message);
		assertThat(converted, instanceOf(Foo.class));

		converter = new ContentTypeDelegatingMessageConverter(null); // no default
		try {
			converter.toMessage(foo, props);
			fail("Expected exception");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(MessageConversionException.class));
			assertThat(e.getMessage(), containsString("No delegate converter"));
		}
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
