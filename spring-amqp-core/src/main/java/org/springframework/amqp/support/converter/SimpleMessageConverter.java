/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.remoting.rmi.CodebaseAwareObjectInputStream;
import org.springframework.util.ClassUtils;

/**
 * Implementation of {@link MessageConverter} that can work with Strings, Serializable instances,
 * or byte arrays. The {@link #toMessage(Object, MessageProperties)} method simply checks the
 * type of the provided instance while the {@link #fromMessage(Message)} method relies upon the
 * {@link MessageProperties#getContentType() content-type} of the provided Message.
 * 
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 */
public class SimpleMessageConverter implements MessageConverter, BeanClassLoaderAware {

	public static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;

	private String codebaseUrl;

	private ClassLoader beanClassLoader = ClassUtils.getDefaultClassLoader();
	
	public void setBeanClassLoader(ClassLoader beanClassLoader) {
		this.beanClassLoader = beanClassLoader;
	}

	/**
	 * Set the codebase URL to download classes from if not found locally. Can consists of multiple URLs, separated by
	 * spaces.
	 * <p>
	 * Follows RMI's codebase conventions for dynamic class download.
	 * 
	 * @see org.springframework.remoting.rmi.CodebaseAwareObjectInputStream
	 * @see java.rmi.server.RMIClassLoader
	 */
	public void setCodebaseUrl(String codebaseUrl) {
		this.codebaseUrl = codebaseUrl;
	}

	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 */
	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}

	/**
	 * Converts from a AMQP Message to an Object.
	 */
	public Object fromMessage(Message message) throws MessageConversionException {
		Object content = null;
		MessageProperties properties = message.getMessageProperties();
		if (properties != null) {
			String contentType = properties.getContentType();
			if (contentType != null && contentType.startsWith("text")) {
				String encoding = properties.getContentEncoding();
				if (encoding == null) {
					encoding = this.defaultCharset;
				}
				try {
					content = new String(message.getBody(), encoding);
				}
				catch (UnsupportedEncodingException e) {
					throw new MessageConversionException(
							"failed to convert text-based Message content", e);
				} 
			}
			else if (contentType != null &&
					contentType.equals(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT)) {
				try {
					content = SerializationUtils.deserialize(createObjectInputStream(new ByteArrayInputStream(message.getBody()), this.codebaseUrl));
				} catch (IOException e) {
					throw new MessageConversionException(
							"failed to convert serialized Message content", e);
				} catch (IllegalArgumentException e) {
					throw new MessageConversionException(
							"failed to convert serialized Message content", e);					
				}
			}
		}
		if (content == null) {
			content = message.getBody();
		}
		return content;
	}

	/**
	 * Creates an AMQP Message from the provided Object.
	 */
	public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
		byte[] bytes = null;		
		if (object instanceof byte[]) {
			bytes = (byte[]) object;
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_BYTES);
		}
		else if (object instanceof String) {
			try {
				bytes = ((String) object).getBytes(this.defaultCharset);
			}
			catch (UnsupportedEncodingException e) {
				throw new MessageConversionException(
						"failed to convert to Message content", e);
			}
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
			messageProperties.setContentEncoding(this.defaultCharset);
		}
		else if (object instanceof Serializable) {
			try {
				bytes = SerializationUtils.serialize(object);
			} catch (IllegalArgumentException e) {
				throw new MessageConversionException(
						"failed to convert to serialized Message content", e);					
			}
			messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
		}
		if (bytes != null) {
			messageProperties.setContentLength(bytes.length);
		}
		return new Message(bytes, messageProperties);
	}

	/**
	 * Create an ObjectInputStream for the given InputStream and codebase. The default implementation creates a
	 * CodebaseAwareObjectInputStream.
	 * @param is the InputStream to read from
	 * @param codebaseUrl the codebase URL to load classes from if not found locally (can be <code>null</code>)
	 * @return the new ObjectInputStream instance to use
	 * @throws IOException if creation of the ObjectInputStream failed
	 * @see org.springframework.remoting.rmi.CodebaseAwareObjectInputStream
	 */
	protected ObjectInputStream createObjectInputStream(InputStream is, String codebaseUrl) throws IOException {
		return new CodebaseAwareObjectInputStream(is, this.beanClassLoader, codebaseUrl);
	}

}
