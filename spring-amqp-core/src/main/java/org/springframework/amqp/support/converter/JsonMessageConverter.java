/*
 * Copyright 2002-2012 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package org.springframework.amqp.support.converter;

import static org.codehaus.jackson.map.type.TypeFactory.type;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * JSON converter that uses the Jackson Json library.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Dave Syer
 * @author Sam Nelson
 */
public class JsonMessageConverter extends AbstractMessageConverter {

	private static Log log = LogFactory.getLog(JsonMessageConverter.class);

	public static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;

	private ObjectMapper jsonObjectMapper = new ObjectMapper();

	private JavaTypeMapper javaTypeMapper = new DefaultJavaTypeMapper();

	private ClassMapper classMapper = null;

	public ClassMapper getClassMapper() {
		return classMapper;

	}

	public void setClassMapper(ClassMapper classMapper) {
		this.classMapper = classMapper;
	}

	public JsonMessageConverter() {
		super();
		initializeJsonObjectMapper();
	}

	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 */
	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset
				: DEFAULT_CHARSET;
	}

	public JavaTypeMapper getJavaTypeMapper() {
		return javaTypeMapper;
	}

	public void setJavaTypeMapper(JavaTypeMapper javaTypeMapper) {
		this.javaTypeMapper = javaTypeMapper;
	}

	/**
	 * The {@link ObjectMapper} to use instead of using the default. An
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
		jsonObjectMapper
				.configure(
						DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
						false);
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
					encoding = this.defaultCharset;
				}
				try {

					if (getClassMapper() == null) {
						JavaType targetJavaType = getJavaTypeMapper()
								.toJavaType(message.getMessageProperties());
						content = convertBytesToObject(message.getBody(),
								encoding, targetJavaType);
					} else {
						Class<?> targetClass = getClassMapper().toClass(
								message.getMessageProperties());
						content = convertBytesToObject(message.getBody(),
								encoding, targetClass);
					}
				} catch (UnsupportedEncodingException e) {
					throw new MessageConversionException(
							"Failed to convert json-based Message content", e);
				} catch (JsonParseException e) {
					throw new MessageConversionException(
							"Failed to convert Message content", e);
				} catch (JsonMappingException e) {
					throw new MessageConversionException(
							"Failed to convert Message content", e);
				} catch (IOException e) {
					throw new MessageConversionException(
							"Failed to convert Message content", e);
				}
			} else {
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
		return jsonObjectMapper.readValue(contentAsString, targetJavaType);
	}

	private Object convertBytesToObject(byte[] body, String encoding,
			Class<?> targetClass) throws JsonParseException,
			JsonMappingException, IOException {
		String contentAsString = new String(body, encoding);
		return jsonObjectMapper.readValue(contentAsString, type(targetClass));
	}

	@Override
	protected Message createMessage(Object objectToConvert,
			MessageProperties messageProperties)
			throws MessageConversionException {
		byte[] bytes = null;
		try {
			String jsonString = jsonObjectMapper
					.writeValueAsString(objectToConvert);
			bytes = jsonString.getBytes(this.defaultCharset);
		} catch (UnsupportedEncodingException e) {
			throw new MessageConversionException(
					"Failed to convert Message content", e);
		} catch (JsonGenerationException e) {
			throw new MessageConversionException(
					"Failed to convert Message content", e);
		} catch (JsonMappingException e) {
			throw new MessageConversionException(
					"Failed to convert Message content", e);
		} catch (IOException e) {
			throw new MessageConversionException(
					"Failed to convert Message content", e);
		}
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
		messageProperties.setContentEncoding(this.defaultCharset);
		if (bytes != null) {
			messageProperties.setContentLength(bytes.length);
		}

		if (getClassMapper() == null) {
			getJavaTypeMapper().fromJavaType(type(objectToConvert.getClass()),
					messageProperties);

		} else {
			getClassMapper().fromClass(objectToConvert.getClass(),
					messageProperties);

		}

		return new Message(bytes, messageProperties);

	}

}
