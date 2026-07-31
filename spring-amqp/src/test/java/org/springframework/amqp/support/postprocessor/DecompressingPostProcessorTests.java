/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.support.postprocessor;

import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Glenn Renfro
 *
 * @since 2.4.19
 */
class DecompressingPostProcessorTests {

	private static final Integer DEFAULT_SIZE = 200 * 1024;

	private GUnzipPostProcessor postProcessor;

	private Message message;

	@BeforeEach
	void setup() throws Exception {

		this.postProcessor = new GUnzipPostProcessor(true);

		byte[] payload = new byte[DEFAULT_SIZE];
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
			gzipOutputStream.write(payload);
		}

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentEncoding("gzip");
		this.message = new Message(outputStream.toByteArray(), messageProperties);
	}

	@Test
	void withinDefaultDecompressedSizeLimit() {
		Message decompressed = this.postProcessor.postProcessMessage(this.message);
		assertThat(decompressed.getBody()).hasSize(DEFAULT_SIZE);
	}

	@Test
	void maxLimitedDecompressedSize() {
		this.postProcessor.setMaxDecompressedSize(250 * 1024);
		Message decompressed = this.postProcessor.postProcessMessage(this.message);
		assertThat(decompressed.getBody()).hasSize(DEFAULT_SIZE);
	}

	@Test
	void exceedsMaxDecompressedSize() {
		int maxDecompressedSize = 100 * 1024;
		this.postProcessor.setMaxDecompressedSize(maxDecompressedSize);
		assertThatExceptionOfType(MessageConversionException.class)
				.isThrownBy(() -> this.postProcessor.postProcessMessage(this.message))
				.withMessageContaining("Decompressed message size exceeds the maximum allowed limit of "
						+ maxDecompressedSize + " bytes");
	}

}
