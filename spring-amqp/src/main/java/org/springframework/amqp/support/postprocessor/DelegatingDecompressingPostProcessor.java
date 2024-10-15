/*
 * Copyright 2014-2024 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.core.Ordered;

/**
 * A {@link MessagePostProcessor} that delegates to one of its {@link MessagePostProcessor}s
 * depending on the content encoding. Supports {@code gzip, zip, deflate} by default.
 *
 * @author Gary Russell
 * @author David Diehl
 * @author Ngoc Nhan
 * @since 1.4.2
 */
public class DelegatingDecompressingPostProcessor implements MessagePostProcessor, Ordered {

	private final Map<String, MessagePostProcessor> decompressors = new HashMap<>();

	private int order;

	/**
	 * Construct an instance with the default decompressors (gzip, zip, deflate) with
	 * the alwaysDecompress flag set to true.
	 */
	public DelegatingDecompressingPostProcessor() {
		this.decompressors.put("gzip", new GUnzipPostProcessor(true));
		this.decompressors.put("zip", new UnzipPostProcessor(true));
		this.decompressors.put("deflate", new InflaterPostProcessor(true));
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	/**
	 * Set the order.
	 * @param order the order.
	 * @see Ordered
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	/**
	 * Add a message post processor to the map of decompressing MessageProcessors.
	 * @param contentEncoding the content encoding; messages will be decompressed with this post processor
	 * if its {@code content-encoding} property matches, or begins with this key followed by ":".
	 * @param decompressor the decompressing {@link MessagePostProcessor}.
	 */
	public void addDecompressor(String contentEncoding, MessagePostProcessor decompressor) {
		this.decompressors.put(contentEncoding, decompressor);
	}

	/**
	 * Remove the decompressor for this encoding; content will not be decompressed even if the
	 * {@link org.springframework.amqp.core.MessageProperties#SPRING_AUTO_DECOMPRESS} header is true.
	 * @param contentEncoding the content encoding.
	 * @return the decompressor if it was present.
	 */
	public MessagePostProcessor removeDecompressor(String contentEncoding) {
		return this.decompressors.remove(contentEncoding);
	}

	/**
	 * Replace all the decompressors.
	 * @param decompressors the decompressors.
	 */
	public void setDecompressors(Map<String, MessagePostProcessor> decompressors) {
		this.decompressors.clear();
		this.decompressors.putAll(decompressors);
	}

	@Override
	public Message postProcessMessage(Message message) throws AmqpException {
		String encoding = message.getMessageProperties().getContentEncoding();
		if (encoding == null) {
			return message;
		}

		int delimAt = encoding.indexOf(':');
		if (delimAt < 0) {
			delimAt = encoding.indexOf(',');
		}
		if (delimAt > 0) {
			encoding = encoding.substring(0, delimAt);
		}
		MessagePostProcessor decompressor = this.decompressors.get(encoding);
		if (decompressor != null) {
			return decompressor.postProcessMessage(message);
		}

		return message;
	}

}
