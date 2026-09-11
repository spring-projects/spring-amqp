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

package org.springframework.amqp.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ngoc Nhan
 *
 * @since 4.2
 */
public final class JacksonUtilsTests {

	@ParameterizedTest
	@ValueSource(strings = {
			"application/json",
			"application/problem+json",
			"application/vnd.api+json",
			"text/x-json"
	})
	void shouldReturnTrueForSupportedJsonContentType(String contentType) {
		assertThat(JacksonUtils.isJsonSupported(MimeType.valueOf(contentType))).isTrue();
	}

	@ParameterizedTest
	@NullSource
	@ValueSource(strings = {
			"application/xml",
			"text/plain",
			"text/html",
			"application/octet-stream",
			"text/xml",
			"text/json",
			"text/foo-json"
	})
	void shouldReturnFalseForUnsupportedJsonContentType(String contentType) {
		MimeType mimeType = contentType == null ? null : MimeType.valueOf(contentType);
		assertThat(JacksonUtils.isJsonSupported(mimeType)).isFalse();
	}

}
