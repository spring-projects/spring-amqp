/*
 * Copyright 2014 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;

/**
 * A {@link MessagePostProcessor} that delegates to one of its {@link MessagePostProcessor}s
 * depending on the content encoding. Supports {@code gzip, zip} by default.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public class DelegatingInflatingPostProcessor implements MessagePostProcessor {

	private final Map<String, MessagePostProcessor> inflaters = new HashMap<String, MessagePostProcessor>();

	public DelegatingInflatingPostProcessor() {
		this.inflaters.put("gzip", new GUnzipPostProcessor());
		this.inflaters.put("zip", new UnzipPostProcessor());
	}

	/**
	 * Add a message post processor to the map of inflaters.
	 * @param contentEncoding the content encoding; messages will be inflated with this post processor
	 * if its {@code content-encoding} property matches, or begins with this key followed by ":".
	 * @param inflater the inflating {@link MessagePostProcessor}.
	 */
	public void addInflater(String contentEncoding, MessagePostProcessor inflater) {
		this.inflaters.put(contentEncoding, inflater);
	}

	/**
	 * Remove the inflater for this encoding; content will not be inflated even if the
	 * {@link MessageProperties#SPRING_AUTO_DECOMPRESS} header is true.
	 * @param contentEncoding the content encoding.
	 * @return the inflater if it was present was present.
	 */
	public MessagePostProcessor removeInflater(String contentEncoding) {
		return this.inflaters.remove(contentEncoding);
	}

	/**
	 * Replace all the inflaters.
	 * @param inflaters the inflaters.
	 */
	public void setInflaters(Map<String, MessagePostProcessor> inflaters) {
		this.inflaters.clear();
		this.inflaters.putAll(inflaters);
	}

	@Override
	public Message postProcessMessage(Message message) throws AmqpException {
		String encoding = message.getMessageProperties().getContentEncoding();
		int colonAt = encoding.indexOf(":");
		if (colonAt > 0) {
			encoding = encoding.substring(0, colonAt);
		}
		MessagePostProcessor inflater = this.inflaters.get(encoding);
		if (inflater != null) {
			return inflater.postProcessMessage(message);
		}
		else {
			return message;
		}
	}

}
