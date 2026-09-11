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

package org.springframework.amqp.support.converter;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link Jackson2JsonMessageConverter} to preserve the behavior between Jackson 2 and Jackson 3
 * until {@link Jackson2JsonMessageConverter} is removed.
 *
 * @author Ngoc Nhan
 * @deprecated since 4.0 in favor of {@link JacksonJsonMessageConverter} for Jackson 3.
 */
@Deprecated(forRemoval = true, since = "4.0")
public class Jackson2JsonMessageConverterTests {

	@ParameterizedTest
	@CsvSource(textBlock = """
			text/x-json,application/json
			application/json,application/json
			text/x-json,text/x-json
			""")
	public void convertMessageWhenContentTypeIsSupported(String contentType, String supportedContentType) {

		byte[] bytes = "{\"message\" : \"Hello, World\"}".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType(contentType);
		Message message = new Message(bytes, messageProperties);

		DefaultClassMapper classMapper = new DefaultClassMapper();
		classMapper.setDefaultType(TestData.class);

		Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
		converter.setAssumeSupportedContentType(false);
		converter.setSupportedContentType(MimeType.valueOf(supportedContentType));
		converter.setClassMapper(classMapper);

		Object foo = converter.fromMessage(message);
		assertThat(foo).isExactlyInstanceOf(TestData.class)
				.extracting("message", InstanceOfAssertFactories.STRING)
				.isEqualTo("Hello, World");
	}

	@Test
	public void returnMessageBodyWhenContentTypeIsNotSupported() {

		byte[] bytes = "{\"message\" : \"Hello, World\"}".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
		Message message = new Message(bytes, messageProperties);

		DefaultClassMapper classMapper = new DefaultClassMapper();
		classMapper.setDefaultType(TestData.class);

		Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
		converter.setAssumeSupportedContentType(false);
		converter.setSupportedContentType(MimeType.valueOf("text/x-json"));
		converter.setClassMapper(classMapper);

		Object foo = converter.fromMessage(message);
		assertThat(foo).isNotExactlyInstanceOf(TestData.class).isSameAs(bytes);
	}

	record TestData(String message) {

	}

}
