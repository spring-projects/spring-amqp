/*
 * Copyright 2002-2016 the original author or authors.
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper.TypePrecedence;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON converter that uses the Jackson 2 Json library.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Dave Syer
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Gary Russell
 * @author Artem Bilan
 */
public class Jackson2JsonMessageConverter extends AbstractJsonMessageConverter {

	private static Log log = LogFactory.getLog(Jackson2JsonMessageConverter.class);

	private ObjectMapper jsonObjectMapper = new ObjectMapper();

	private Jackson2JavaTypeMapper javaTypeMapper = new DefaultJackson2JavaTypeMapper();

	private boolean typeMapperSet;

	public Jackson2JsonMessageConverter() {
		initializeJsonObjectMapper();
	}

	public Jackson2JavaTypeMapper getJavaTypeMapper() {
		return this.javaTypeMapper;
	}

	public void setJavaTypeMapper(Jackson2JavaTypeMapper javaTypeMapper) {
		this.javaTypeMapper = javaTypeMapper;
		this.typeMapperSet = true;
	}

	/**
	 * The {@link com.fasterxml.jackson.databind.ObjectMapper} to use instead of using the default. An
	 * alternative to injecting a mapper is to extend this class and override
	 * {@link #initializeJsonObjectMapper()}.
	 * @param jsonObjectMapper the object mapper to set
	 */
	public void setJsonObjectMapper(ObjectMapper jsonObjectMapper) {
		this.jsonObjectMapper = jsonObjectMapper;
	}

	/**
	 * @return the precedence.
	 * @see #setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence)
	 * @since 1.6.
	 */
	public Jackson2JavaTypeMapper.TypePrecedence getTypePrecedence() {
		return this.javaTypeMapper.getTypePrecedence();
	}

	/**
	 * Set the precedence for evaluating type information in message properties.
	 * When using {@code @RabbitListener} at the method level, the framework attempts
	 * to determine the target type for payload conversion from the method signature.
	 * If so, this type is provided in the
	 * {@link MessageProperties#getInferredArgumentType() inferredArgumentType}
	 * message property.
	 * <p> By default, if the type is concrete (not abstract, not an interface), this will
	 * be used ahead of type information provided in the {@code __TypeId__} and
	 * associated headers provided by the sender.
	 * <p> If you wish to force the use of the  {@code __TypeId__} and associated headers
	 * (such as when the actual type is a subclass of the method argument type),
	 * set the precedence to {@link TypePrecedence#TYPE_ID}.
	 * @param typePrecedence the precedence.
	 * @since 1.6
	 * @see DefaultJackson2JavaTypeMapper#setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence)
	 */
	public void setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence typePrecedence) {
		if (this.typeMapperSet) {
			throw new IllegalStateException("When providing your own type mapper, you should set the precedence on it");
		}
		if (this.javaTypeMapper instanceof DefaultJackson2JavaTypeMapper) {
			((DefaultJackson2JavaTypeMapper) this.javaTypeMapper).setTypePrecedence(typePrecedence);
		}
		else {
			throw new IllegalStateException("Type precedence is available with the DefaultJackson2JavaTypeMapper");
		}
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		super.setBeanClassLoader(classLoader);
		if (!this.typeMapperSet) {
			((DefaultJackson2JavaTypeMapper) this.javaTypeMapper).setBeanClassLoader(classLoader);
		}
	}

	/**
	 * Subclass and override to customize.
	 */
	protected void initializeJsonObjectMapper() {
		this.jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Override
	public Object fromMessage(Message message)
			throws MessageConversionException {
		Object content = null;
		MessageProperties properties = message.getMessageProperties();
		if (properties != null) {
			String contentType = properties.getContentType();
			if (contentType != null && contentType.contains("json")) {
				String encoding = properties.getContentEncoding();
				if (encoding == null) {
					encoding = getDefaultCharset();
				}
				try {

					if (getClassMapper() == null) {
						JavaType targetJavaType = getJavaTypeMapper()
								.toJavaType(message.getMessageProperties());
						content = convertBytesToObject(message.getBody(),
								encoding, targetJavaType);
					}
					else {
						Class<?> targetClass = getClassMapper().toClass(
								message.getMessageProperties());
						content = convertBytesToObject(message.getBody(),
								encoding, targetClass);
					}
				}
				catch (IOException e) {
					throw new MessageConversionException(
							"Failed to convert Message content", e);
				}
			}
			else {
				if (log.isWarnEnabled()) {
					log.warn("Could not convert incoming message with content-type ["
							+ contentType + "]");
				}
			}
		}
		if (content == null) {
			content = message.getBody();
		}
		return content;
	}

	private Object convertBytesToObject(byte[] body, String encoding, JavaType targetJavaType) throws IOException {
		String contentAsString = new String(body, encoding);
		return this.jsonObjectMapper.readValue(contentAsString, targetJavaType);
	}

	private Object convertBytesToObject(byte[] body, String encoding, Class<?> targetClass) throws IOException {
		String contentAsString = new String(body, encoding);
		return this.jsonObjectMapper.readValue(contentAsString, this.jsonObjectMapper.constructType(targetClass));
	}

	@Override
	protected Message createMessage(Object objectToConvert, MessageProperties messageProperties)
			throws MessageConversionException {
		byte[] bytes;
		try {
			String jsonString = this.jsonObjectMapper
					.writeValueAsString(objectToConvert);
			bytes = jsonString.getBytes(getDefaultCharset());
		}
		catch (IOException e) {
			throw new MessageConversionException(
					"Failed to convert Message content", e);
		}
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
		messageProperties.setContentEncoding(getDefaultCharset());
		messageProperties.setContentLength(bytes.length);

		if (getClassMapper() == null) {
			getJavaTypeMapper().fromJavaType(this.jsonObjectMapper.constructType(objectToConvert.getClass()),
					messageProperties);

		}
		else {
			getClassMapper().fromClass(objectToConvert.getClass(),
					messageProperties);

		}

		return new Message(bytes, messageProperties);
	}

}
