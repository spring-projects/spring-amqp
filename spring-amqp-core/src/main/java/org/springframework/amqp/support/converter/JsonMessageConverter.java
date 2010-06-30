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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * JSON converter that uses the Jackson Json library.
 * 
 * @author Mark Pollack
 */
public class JsonMessageConverter implements MessageConverter {

	// TODO create composite MessageConverter with key/value pairs of content-type/converter.  introduce base class
	
	private static Log log = LogFactory.getLog(JsonMessageConverter.class);

	public static final String DEFAULT_CHARSET = "UTF-8";


	private volatile String defaultCharset = DEFAULT_CHARSET;
	
	private ObjectMapper jsonObjectMapper = new ObjectMapper();
	
	private ClassMapper classMapper = new DefaultClassMapper();


	public JsonMessageConverter() {
		super();
		initializeJsonObjectMapper();
	}


	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 */
	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}

	public ClassMapper getClassMapper() {
		return classMapper;
	}

	public void setClassMapper(ClassMapper classMapper) {
		this.classMapper = classMapper;
	}

	/**
	 * Subclass and override to customize.
	 */
	protected void initializeJsonObjectMapper() {
		jsonObjectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}


	public Object fromMessage(Message message) throws MessageConversionException {
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
					//content = new String(message.getBody(), encoding);
					Map<String, Object> headers =  message.getMessageProperties().getHeaders();
					Object classIdFieldNameValue = headers.get(classMapper.getClassIdFieldName());
					String classId = null;
					if (classIdFieldNameValue != null) {
						classId = classIdFieldNameValue.toString();
					}					
					if (classId == null) {
						throw new MessageConversionException(
								"failed to convert json-based Message content. Could not resolve classId in header");
					}
					Class<?> targetClass = classMapper.toClass(classId);
					content = convertBytesToObject(message.getBody(), encoding, targetClass);
				}
				catch (UnsupportedEncodingException e) {
					throw new MessageConversionException("Failed to convert json-based Message content", e);
				}
				catch (JsonParseException e) {
					throw new MessageConversionException("Failed to convert Message content", e);
				}
				catch (JsonMappingException e) {
					throw new MessageConversionException("Failed to convert Message content", e);
				}
				catch (IOException e) {
					throw new MessageConversionException("Failed to convert Message content", e);
				} 
			}
			else {
				log.warn("Could not convert incoming message with content-type [" + contentType + "]");
			}
		}
		if (content == null) {
			content = message.getBody();
		}
		return content;
	}


	private Object convertBytesToObject(byte[] body, String encoding, Class<?> targetClass) throws JsonParseException, JsonMappingException, IOException {
		String contentAsString = new String(body, encoding);
		return jsonObjectMapper.readValue(contentAsString, targetClass);	
	}

	public Message toMessage(Object objectToConvert, MessageProperties messageProperties) throws MessageConversionException {
		byte[] bytes = null;
		try {
			String jsonString = jsonObjectMapper.writeValueAsString(objectToConvert);
			bytes = jsonString.getBytes(this.defaultCharset);
		}
		catch (UnsupportedEncodingException e) {
			throw new MessageConversionException("Failed to convert Message content", e);
		}
		catch (JsonGenerationException e) {
			throw new MessageConversionException("Failed to convert Message content", e);
		}
		catch (JsonMappingException e) {
			throw new MessageConversionException("Failed to convert Message content", e);
		}
		catch (IOException e) {
			throw new MessageConversionException("Failed to convert Message content", e);
		}
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
		messageProperties.setContentEncoding(this.defaultCharset);
		if (bytes != null) {
			messageProperties.setContentLength(bytes.length);
		}
		Map<String, Object> headers =  messageProperties.getHeaders();
		headers.put(classMapper.getClassIdFieldName(), classMapper.fromClass(objectToConvert.getClass()));
		return new Message(bytes, messageProperties);
	}

}
