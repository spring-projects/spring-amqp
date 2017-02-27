/*
 * Copyright 2014-2017 the original author or authors.
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
import org.springframework.core.Ordered;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

/**
 * Base class for post processors that decompress the message body if the
 * {@link MessageProperties#SPRING_AUTO_DECOMPRESS} header is true or to optionally always
 * decompress if the content encoding matches {@link #getEncoding()}, or starts with
 * {@link #getEncoding()} + ":", in which case the encoding following the colon becomes
 * the final content encoding of the decompressed message.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public abstract class AbstractDecompressingPostProcessor implements MessagePostProcessor, Ordered {

	private final boolean alwaysDecompress;

	private int order;

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

	@Override
	public Message postProcessMessage(Message message) throws AmqpException {
		Object autoDecompress = message.getMessageProperties().getHeaders()
				.get(MessageProperties.SPRING_AUTO_DECOMPRESS);
		if (this.alwaysDecompress || (autoDecompress instanceof Boolean && ((Boolean) autoDecompress))) {
			ByteArrayInputStream zipped = new ByteArrayInputStream(message.getBody());
			try {
				InputStream unzipper = getDecompressorStream(zipped);
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				FileCopyUtils.copy(unzipper, out);
				MessageProperties messageProperties = message.getMessageProperties();
				String encoding = messageProperties.getContentEncoding();
				int colonAt = encoding.indexOf(':');
				if (colonAt > 0) {
					encoding = encoding.substring(0, colonAt);
				}
				Assert.state(getEncoding().equals(encoding), "Content encoding must be:" + getEncoding() + ", was:"
						+ encoding);
				if (colonAt < 0) {
					messageProperties.setContentEncoding(null);
				}
				else {
					messageProperties.setContentEncoding(messageProperties.getContentEncoding().substring(colonAt + 1));
				}
				messageProperties.getHeaders().remove(MessageProperties.SPRING_AUTO_DECOMPRESS);
				return new Message(out.toByteArray(), messageProperties);
			}
			catch (IOException e) {
				throw new AmqpIOException(e);
			}
		}
		else {
			return message;
		}
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
