/*
 * Copyright 2002-2018 the original author or authors.
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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Abstract Jackson2 message converter
 *
 * @author Mohammad Hewedy
 */
public abstract class AbstractJackson2MessageConverter extends AbstractMessageConverter
		implements BeanClassLoaderAware, SmartMessageConverter {

	private static Log log = LogFactory.getLog(AbstractJackson2MessageConverter.class);

	public static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;

	private ClassMapper classMapper = null;

	protected boolean typeMapperSet;

	private final MimeType contentType;

	protected final ObjectMapper objectMapper;

	private ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	private Jackson2JavaTypeMapper javaTypeMapper = new DefaultJackson2JavaTypeMapper();

	/**
	 * Construct with the provided {@link ObjectMapper} instance.
	 * @param objectMapper the {@link ObjectMapper} to use.
	 * @param contentType content type of the message
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @since 1.6.11
	 * @see DefaultJackson2JavaTypeMapper#setTrustedPackages(String...)
	 */
	public AbstractJackson2MessageConverter(ObjectMapper objectMapper, MimeType contentType, String... trustedPackages) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null");
		this.objectMapper = objectMapper;
		this.contentType = contentType;
		((DefaultJackson2JavaTypeMapper) this.javaTypeMapper).setTrustedPackages(trustedPackages);
	}

	public ClassMapper getClassMapper() {
		return this.classMapper;

	}

	public void setClassMapper(ClassMapper classMapper) {
		this.classMapper = classMapper;
	}

	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 *
	 * @param defaultCharset The default charset.
	 */
	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset
				: DEFAULT_CHARSET;
	}

	public String getDefaultCharset() {
		return this.defaultCharset;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		if (!this.typeMapperSet) {
			((DefaultJackson2JavaTypeMapper) this.javaTypeMapper).setBeanClassLoader(classLoader);
		}
	}

	protected ClassLoader getClassLoader() {
		return this.classLoader;
	}

	public Jackson2JavaTypeMapper getJavaTypeMapper() {
		return this.javaTypeMapper;
	}

	public void setJavaTypeMapper(Jackson2JavaTypeMapper javaTypeMapper) {
		this.javaTypeMapper = javaTypeMapper;
		this.typeMapperSet = true;
	}

	/**
	 * Return the type precedence.
	 * @return the precedence.
	 * @since 1.6.
	 * @see #setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence)
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
	 * set the precedence to {@link Jackson2JavaTypeMapper.TypePrecedence#TYPE_ID}.
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
	public Object fromMessage(Message message) throws MessageConversionException {
		return fromMessage(message, null);
	}

	/**
	 * {@inheritDoc}
	 * @param conversionHint The conversionHint must be a {@link ParameterizedTypeReference}.
	 */
	@Override
	public Object fromMessage(Message message, Object conversionHint) throws MessageConversionException {
		Object content = null;
		MessageProperties properties = message.getMessageProperties();
		if (properties != null) {
			String contentType = properties.getContentType();
			if (contentType != null && contentType.contains(this.contentType.getSubtype())) {
				String encoding = properties.getContentEncoding();
				if (encoding == null) {
					encoding = getDefaultCharset();
				}
				try {
					if (conversionHint instanceof ParameterizedTypeReference) {
						content = convertBytesToObject(message.getBody(), encoding,
								this.objectMapper.getTypeFactory().constructType(
										((ParameterizedTypeReference<?>) conversionHint).getType()));
					}
					else if (getClassMapper() == null) {
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
							+ contentType + "], '" + this.contentType.getSubtype() + "' keyword missing.");
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
		return this.objectMapper.readValue(contentAsString, targetJavaType);
	}

	private Object convertBytesToObject(byte[] body, String encoding, Class<?> targetClass) throws IOException {
		String contentAsString = new String(body, encoding);
		return this.objectMapper.readValue(contentAsString, this.objectMapper.constructType(targetClass));
	}

	@Override
	protected Message createMessage(Object objectToConvert, MessageProperties messageProperties)
			throws MessageConversionException {

		return createMessage(objectToConvert, messageProperties, null);
	}

	@Override
	protected Message createMessage(Object objectToConvert, MessageProperties messageProperties, Type genericType)
			throws MessageConversionException {

		byte[] bytes;
		try {
			String jsonString = this.objectMapper
					.writeValueAsString(objectToConvert);
			bytes = jsonString.getBytes(getDefaultCharset());
		}
		catch (IOException e) {
			throw new MessageConversionException("Failed to convert Message content", e);
		}
		messageProperties.setContentType(this.contentType.toString());
		messageProperties.setContentEncoding(getDefaultCharset());
		messageProperties.setContentLength(bytes.length);

		if (getClassMapper() == null) {
			getJavaTypeMapper().fromJavaType(this.objectMapper.constructType(
					genericType == null ? objectToConvert.getClass() : genericType), messageProperties);
		}
		else {
			getClassMapper().fromClass(objectToConvert.getClass(),
					messageProperties);
		}

		return new Message(bytes, messageProperties);
	}
}
