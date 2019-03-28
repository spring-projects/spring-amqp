/*
 * Copyright 2014-2019 the original author or authors.
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
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilderSupport;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.core.Ordered;
import org.springframework.util.FileCopyUtils;

/**
 * Base class for post processors that compress the message body. The content encoding is
 * set to {@link #getEncoding()} or {@link #getEncoding()} + ":" + existing encoding, if
 * present.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4.2
 */
public abstract class AbstractCompressingPostProcessor implements MessagePostProcessor, Ordered {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR final

	private final boolean autoDecompress;

	private int order;

	/**
	 * Construct a post processor that will include the
	 * {@link MessageProperties#SPRING_AUTO_DECOMPRESS} header set to 'true'.
	 */
	public AbstractCompressingPostProcessor() {
		this(true);
	}

	/**
	 * Construct a post processor that will include (or not include) the
	 * {@link MessageProperties#SPRING_AUTO_DECOMPRESS} header. Used by the (Spring AMQP) inbound
	 * message converter to determine whether the message should be decompressed
	 * automatically, or remain compressed.
	 * @param autoDecompress true to indicate the receiver should automatically
	 * decompress.
	 */
	public AbstractCompressingPostProcessor(boolean autoDecompress) {
		this.autoDecompress = autoDecompress;
	}

	@Override
	public Message postProcessMessage(Message message) throws AmqpException {
		ByteArrayOutputStream zipped = new ByteArrayOutputStream();
		try {
			OutputStream zipper = getCompressorStream(zipped);
			FileCopyUtils.copy(new ByteArrayInputStream(message.getBody()), zipper);
			MessageProperties originalProperties = message.getMessageProperties();
			MessageBuilderSupport<MessageProperties> messagePropertiesMessageBuilder =
					MessagePropertiesBuilder.fromClonedProperties(originalProperties)
							.setContentEncoding(getEncoding() +
									(originalProperties.getContentEncoding() == null
											? ""
											: ":" + originalProperties.getContentEncoding()));

			if (this.autoDecompress) {
				messagePropertiesMessageBuilder.setHeader(MessageProperties.SPRING_AUTO_DECOMPRESS, true);
			}

			MessageProperties messageProperties = messagePropertiesMessageBuilder.build();
			byte[] compressed = zipped.toByteArray();
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("Compressed " + message.getBody().length + " to " + compressed.length);
			}
			return new Message(compressed, messageProperties);
		}
		catch (IOException e) {
			throw new AmqpIOException(e);
		}
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
	 * Get the stream.
	 * @param stream The output stream to write the compressed data to.
	 * @return the compressor output stream.
	 * @throws IOException IOException
	 */
	protected abstract OutputStream getCompressorStream(OutputStream stream) throws IOException;

	/**
	 * Get the encoding.
	 * @return the content-encoding header.
	 */
	protected abstract String getEncoding();

}
