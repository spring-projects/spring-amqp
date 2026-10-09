/*
 * Copyright 2015-present the original author or authors.
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
