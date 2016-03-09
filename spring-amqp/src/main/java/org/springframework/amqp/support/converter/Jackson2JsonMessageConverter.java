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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON converter that uses the Jackson 2 Json library.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Dave Syer
 * @author Sam Nelson
 * @author Andreas Asplund
 */
public class Jackson2JsonMessageConverter extends AbstractJsonMessageConverter {

	private static Log log = LogFactory.getLog(Jackson2JsonMessageConverter.class);

	private ObjectMapper jsonObjectMapper = new ObjectMapper();

	private Jackson2JavaTypeMapper javaTypeMapper = new DefaultJackson2JavaTypeMapper();

	public Jackson2JsonMessageConverter() {
		super();
		initializeJsonObjectMapper();
	}

	public Jackson2JavaTypeMapper getJavaTypeMapper() {
		return this.javaTypeMapper;
	}

	public void setJavaTypeMapper(Jackson2JavaTypeMapper javaTypeMapper) {
		this.javaTypeMapper = javaTypeMapper;
	}

	/**
	 * The {@link com.fasterxml.jackson.databind.ObjectMapper} to use instead of using the default. An
	 * alternative to injecting a mapper is to extend this class and override
	 * {@link #initializeJsonObjectMapper()}.
	 *
	 * @param jsonObjectMapper
	 *            the object mapper to set
	 */
	public void setJsonObjectMapper(ObjectMapper jsonObjectMapper) {
		this.jsonObjectMapper = jsonObjectMapper;
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
				log.warn("Could not convert incoming message with content-type ["
						+ contentType + "]");
			}
		}
		if (content == null) {
			content = message.getBody();
		}
		return content;
	}

	private Object convertBytesToObject(byte[] body, String encoding,
			JavaType targetJavaType) throws JsonParseException,
			JsonMappingException, IOException {
		String contentAsString = new String(body, encoding);
		return this.jsonObjectMapper.readValue(contentAsString, targetJavaType);
	}

	private Object convertBytesToObject(byte[] body, String encoding,
			Class<?> targetClass) throws JsonParseException,
			JsonMappingException, IOException {
		String contentAsString = new String(body, encoding);
		return this.jsonObjectMapper.readValue(contentAsString, this.jsonObjectMapper.constructType(targetClass));
	}

	@Override
	protected Message createMessage(Object objectToConvert,
			MessageProperties messageProperties)
			throws MessageConversionException {
		byte[] bytes = null;
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
		if (bytes != null) {
			messageProperties.setContentLength(bytes.length);
		}

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
