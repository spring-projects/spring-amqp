/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.support.converter;

import java.io.Serializable;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
