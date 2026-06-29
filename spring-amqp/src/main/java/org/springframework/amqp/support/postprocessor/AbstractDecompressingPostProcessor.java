/*
 * Copyright 2014-present the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.core.Ordered;
import org.springframework.util.Assert;

/**
 * Base class for post processors that decompress the message body if the
 * {@link MessageProperties#SPRING_AUTO_DECOMPRESS} header is true or to optionally always
 * decompress if the content encoding matches {@link #getEncoding()}, or starts with
 * {@link #getEncoding()} + ":", in which case the encoding following the colon becomes
 * the final content encoding of the decompressed message.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 * @author Glenn Renfro
 *
 * @since 1.4.2
 */
public abstract class AbstractDecompressingPostProcessor implements MessagePostProcessor, Ordered {

	private static final int BUFFER_SIZE = 16384;

	private final boolean alwaysDecompress;

	private int order;

	private long maxDecompressedSize = 1024 * 1024 * 100;

	/**
	 * Construct a post processor that will decompress the supported content
	 * encoding only if {@link MessageProperties#SPRING_AUTO_DECOMPRESS} header is present
	 * and true.
	 */
	public AbstractDecompressingPostProcessor() {
		this(false);
	}

	/**
	 * Construct a post processor that will decompress the supported content
	 * encoding if {@link MessageProperties#SPRING_AUTO_DECOMPRESS} header is present
	 * and true or if alwaysDecompress is true.
	 * @param alwaysDecompress true to always decompress.
	 */
	public AbstractDecompressingPostProcessor(boolean alwaysDecompress) {
		this.alwaysDecompress = alwaysDecompress;
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	/**
	 * Set the order.
	 * @param order the order, default 0.
	 * @see Ordered
	 */
	protected void setOrder(int order) {
		this.order = order;
	}

	/**
	 * Set the maximum allowed size of the decompressed message.
	 * @param maxDecompressedSize the max decompressed size in bytes; defaults to 100MB.
	 * A value of 0 is unlimited.
	 * @since 2.4.19
	 */
	public void setMaxDecompressedSize(long maxDecompressedSize) {
		Assert.isTrue(maxDecompressedSize >= 0, "'maxDecompressedSize' must not be a negative number");
		this.maxDecompressedSize = maxDecompressedSize;
	}

	@Override
	public Message postProcessMessage(Message message) throws AmqpException {
		Object autoDecompress = message.getMessageProperties().getHeaders()
				.get(MessageProperties.SPRING_AUTO_DECOMPRESS);
		if (this.alwaysDecompress || (autoDecompress instanceof Boolean isAutoDecompress && isAutoDecompress)) {
			ByteArrayInputStream zipped = new ByteArrayInputStream(message.getBody());
			try (InputStream unzipper = getDecompressorStream(zipped)) {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				byte[] buffer = new byte[BUFFER_SIZE];
				int bytesRead;
				long totalBytes = 0L;
				while ((bytesRead = unzipper.read(buffer)) != -1) {
					totalBytes += bytesRead;
					if (this.maxDecompressedSize > 0 && totalBytes > this.maxDecompressedSize) {
						throw new MessageConversionException("Decompressed message size exceeds the maximum allowed " +
								"limit of " + this.maxDecompressedSize + " bytes");
					}
					out.write(buffer, 0, bytesRead);
				}
				MessageProperties messageProperties = message.getMessageProperties();
				String contentEncoding = messageProperties.getContentEncoding();
				Assert.hasText(contentEncoding, "The 'encoding' message property is required");
				String encoding = contentEncoding;
				int delimAt = encoding.indexOf(':');
				if (delimAt < 0) {
					delimAt = encoding.indexOf(',');
				}
				if (delimAt > 0) {
					encoding = encoding.substring(0, delimAt);
				}
				Assert.state(getEncoding().equals(encoding), "Content encoding must be:" + getEncoding() + ", was:"
						+ encoding);
				if (delimAt < 0) {
					messageProperties.setContentEncoding(null);
				}
				else {
					messageProperties.setContentEncoding(contentEncoding.substring(delimAt + 1).trim());
				}
				messageProperties.getHeaders().remove(MessageProperties.SPRING_AUTO_DECOMPRESS);
				return new Message(out.toByteArray(), messageProperties);
			}
			catch (IOException e) {
				throw new AmqpIOException(e);
			}
		}

		return message;
	}

	/**
	 * Get the stream.
	 * @param stream The output stream to write the compressed data to.
	 * @return the decompressor input stream.
	 * @throws IOException IOException
	 */
	protected abstract InputStream getDecompressorStream(InputStream stream) throws IOException;

	/**
	 * Get the encoding.
	 * @return the content-encoding header.
	 */
	protected abstract String getEncoding();

}
