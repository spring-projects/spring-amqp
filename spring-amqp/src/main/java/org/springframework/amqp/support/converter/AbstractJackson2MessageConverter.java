/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract Jackson2 message converter.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Dave Syer
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Artem Bilan
 * @author Mohammad Hewedy
 * @author Gary Russell
 *
 * @since 2.1
 */
public abstract class AbstractJackson2MessageConverter extends AbstractMessageConverter
		implements BeanClassLoaderAware, SmartMessageConverter {

	protected final Log log = LogFactory.getLog(getClass()); // NOSONAR protected

	/**
	 * The charset used when converting {@link String} to/from {@code byte[]}.
	 */
	public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

	protected final ObjectMapper objectMapper; // NOSONAR protected

	/**
	 * The supported content type; only the subtype is checked when decoding, e.g.
	 * *&#47;json, *&#47;xml. If this contains a charset parameter, when encoding, the
	 * contentType header will not be set, when decoding, the raw bytes are passed to
	 * Jackson which can dynamically determine the encoding; otherwise the contentEncoding
	 * or default charset is used.
	 */
	private MimeType supportedContentType;

	private String supportedCTCharset;

	@Nullable
	private ClassMapper classMapper = null;

	private Charset defaultCharset = DEFAULT_CHARSET;

	private boolean typeMapperSet;

	private ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	private Jackson2JavaTypeMapper javaTypeMapper = new DefaultJackson2JavaTypeMapper();

	private boolean useProjectionForInterfaces;

	private ProjectingMessageConverter projectingConverter;

	private boolean charsetIsUtf8 = true;

	private boolean assumeSupportedContentType = true;

	private boolean alwaysConvertToInferredType;

	private boolean nullAsOptionalEmpty;

	/**
	 * Construct with the provided {@link ObjectMapper} instance.
	 * @param objectMapper the {@link ObjectMapper} to use.
	 * @param contentType the supported content type; only the subtype is checked when
	 * decoding, e.g. *&#47;json, *&#47;xml. If this contains a charset parameter, when
	 * encoding, the contentType header will not be set, when decoding, the raw bytes are
	 * passed to Jackson which can dynamically determine the encoding; otherwise the
	 * contentEncoding or default charset is used.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJackson2JavaTypeMapper#setTrustedPackages(String...)
	 */
	protected AbstractJackson2MessageConverter(ObjectMapper objectMapper, MimeType contentType,
			String... trustedPackages) {

		Assert.notNull(objectMapper, "'objectMapper' must not be null");
		Assert.notNull(contentType, "'contentType' must not be null");
		this.objectMapper = objectMapper;
		this.supportedContentType = contentType;
		this.supportedCTCharset = this.supportedContentType.getParameter("charset");
		((DefaultJackson2JavaTypeMapper) this.javaTypeMapper).setTrustedPackages(trustedPackages);
	}


	/**
	 * Get the supported content type; only the subtype is checked when decoding, e.g.
	 * *&#47;json, *&#47;xml. If this contains a charset parameter, when encoding, the
	 * contentType header will not be set, when decoding, the raw bytes are passed to
	 * Jackson which can dynamically determine the encoding; otherwise the contentEncoding
	 * or default charset is used.
	 * @return the supportedContentType
	 * @since 2.4.3
	 */
	protected MimeType getSupportedContentType() {
		return this.supportedContentType;
	}


	/**
	 * Set the supported content type; only the subtype is checked when decoding, e.g.
	 * *&#47;json, *&#47;xml. If this contains a charset parameter, when encoding, the
	 * contentType header will not be set, when decoding, the raw bytes are passed to
	 * Jackson which can dynamically determine the encoding; otherwise the contentEncoding
	 * or default charset is used.
	 * @param supportedContentType the supportedContentType to set.
	 * @since 2.4.3
	 */
	public void setSupportedContentType(MimeType supportedContentType) {
		Assert.notNull(supportedContentType, "'supportedContentType' cannot be null");
		this.supportedContentType = supportedContentType;
		this.supportedCTCharset = this.supportedContentType.getParameter("charset");
	}

	/**
	 * When true, if jackson decodes the body as {@code null} convert to {@link Optional#empty()}
	 * instead of returning the original body. Default false.
	 * @param nullAsOptionalEmpty true to return empty.
	 * @since 2.4.7
	 */
	public void setNullAsOptionalEmpty(boolean nullAsOptionalEmpty) {
		this.nullAsOptionalEmpty = nullAsOptionalEmpty;
	}

	@Nullable
	public ClassMapper getClassMapper() {
		return this.classMapper;
	}

	public void setClassMapper(ClassMapper classMapper) {
		this.classMapper = classMapper;
	}

	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 * @param defaultCharset The default charset.
	 */
	public void setDefaultCharset(@Nullable String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? Charset.forName(defaultCharset)
				: DEFAULT_CHARSET;
		this.charsetIsUtf8 = this.defaultCharset.equals(StandardCharsets.UTF_8);
	}

	public String getDefaultCharset() {
		return this.defaultCharset.name();
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

	/**
	 * Whether or not an explicit java type mapper has been provided.
	 * @return false if the default type mapper is being used.
	 * @since 2.2
	 * @see #setJavaTypeMapper(Jackson2JavaTypeMapper)
	 */
	public boolean isTypeMapperSet() {
		return this.typeMapperSet;
	}

	public void setJavaTypeMapper(Jackson2JavaTypeMapper javaTypeMapper) {
		Assert.notNull(javaTypeMapper, "'javaTypeMapper' cannot be null");
		this.javaTypeMapper = javaTypeMapper;
		this.typeMapperSet = true;
	}

	/**
	 * Return the type precedence.
	 * @return the precedence.
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

	/**
	 * When false (default), fall back to type id headers if the type (or contents of a container
	 * type) is abstract. Set to true if conversion should always be attempted - perhaps because
	 * a custom deserializer has been configured on the {@link ObjectMapper}. If the attempt fails,
	 * fall back to headers.
	 * @param alwaysAttemptConversion true to attempt.
	 * @since 2.2.8
	 */
	public void setAlwaysConvertToInferredType(boolean alwaysAttemptConversion) {
		this.alwaysConvertToInferredType = alwaysAttemptConversion;
	}

	protected boolean isUseProjectionForInterfaces() {
		return this.useProjectionForInterfaces;
	}

	/**
	 * Set to true to use Spring Data projection to create the object if the inferred
	 * parameter type is an interface.
	 * @param useProjectionForInterfaces true to use projection.
	 * @since 2.2
	 */
	public void setUseProjectionForInterfaces(boolean useProjectionForInterfaces) {
		this.useProjectionForInterfaces = useProjectionForInterfaces;
		if (useProjectionForInterfaces) {
			if (!ClassUtils.isPresent("org.springframework.data.projection.ProjectionFactory", this.classLoader)) {
				throw new IllegalStateException("'spring-data-commons' is required to use Projection Interfaces");
			}
			this.projectingConverter = new ProjectingMessageConverter(this.objectMapper);
		}
	}

	/**
	 * By default the supported content type is assumed when there is no contentType
	 * property or it is set to the default ('application/octet-stream'). Set to 'false'
	 * to revert to the previous behavior of returning an unconverted 'byte[]' when this
	 * condition exists.
	 * @param assumeSupportedContentType set false to not assume the content type is
	 * supported.
	 * @since 2.2
	 */
	public void setAssumeSupportedContentType(boolean assumeSupportedContentType) {
		this.assumeSupportedContentType = assumeSupportedContentType;
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
	public Object fromMessage(Message message, @Nullable Object conversionHint) throws MessageConversionException {
		Object content = null;
		MessageProperties properties = message.getMessageProperties();
		if (properties != null) {
			String contentType = properties.getContentType();
			if ((this.assumeSupportedContentType // NOSONAR Boolean complexity
					&& (contentType == null || contentType.equals(MessageProperties.DEFAULT_CONTENT_TYPE)))
					|| (contentType != null && contentType.contains(this.supportedContentType.getSubtype()))) {
				String encoding = determineEncoding(properties, contentType);
				content = doFromMessage(message, conversionHint, properties, encoding);
			}
			else {
				if (this.log.isWarnEnabled()) {
					this.log.warn("Could not convert incoming message with content-type ["
							+ contentType + "], '" + this.supportedContentType.getSubtype() + "' keyword missing.");
				}
			}
		}
		if (content == null) {
			if (this.nullAsOptionalEmpty) {
				content = Optional.empty();
			}
			else {
				content = message.getBody();
			}
		}
		return content;
	}

	private String determineEncoding(MessageProperties properties, @Nullable String contentType) {
		String encoding = properties.getContentEncoding();
		if (encoding == null && contentType != null) {
			try {
				MimeType mimeType = MimeTypeUtils.parseMimeType(contentType);
				if (mimeType != null) {
					encoding = mimeType.getParameter("charset");
				}
			}
			catch (RuntimeException e) {
			}
		}
		if (encoding == null) {
			encoding = this.supportedCTCharset != null ? this.supportedCTCharset : getDefaultCharset();
		}
		return encoding;
	}

	private Object doFromMessage(Message message, Object conversionHint, MessageProperties properties,
			String encoding) {

		Object content = null;
		try {
			content = convertContent(message, conversionHint, properties, encoding);
		}
		catch (IOException e) {
			throw new MessageConversionException(
					"Failed to convert Message content", e);
		}
		return content;
	}

	private Object convertContent(Message message, Object conversionHint, MessageProperties properties, String encoding)
			throws IOException {

		Object content = null;
		JavaType inferredType = this.javaTypeMapper.getInferredType(properties);
		if (inferredType != null && this.useProjectionForInterfaces && inferredType.isInterface()
				&& !inferredType.getRawClass().getPackage().getName().startsWith("java.util")) { // List etc
			content = this.projectingConverter.convert(message, inferredType.getRawClass());
			properties.setProjectionUsed(true);
		}
		else if (inferredType != null && this.alwaysConvertToInferredType) {
			content = tryConverType(message, encoding, inferredType);
		}
		if (content == null) {
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
				Class<?> targetClass = getClassMapper().toClass(// NOSONAR never null
						message.getMessageProperties());
				content = convertBytesToObject(message.getBody(),
						encoding, targetClass);
			}
		}
		return content;
	}

	/*
	 * Unfortunately, mapper.canDeserialize() always returns true (adds an AbstractDeserializer
	 * to the cache); so all we can do is try a conversion.
	 */
	@Nullable
	private Object tryConverType(Message message, String encoding, JavaType inferredType) {
		try {
			return convertBytesToObject(message.getBody(), encoding, inferredType);
		}
		catch (Exception e) {
			this.log.trace("Cannot create possibly abstract container contents; falling back to headers", e);
			return null;
		}
	}

	private Object convertBytesToObject(byte[] body, String encoding, JavaType targetJavaType) throws IOException {
		if (this.supportedCTCharset != null) { // Jackson will determine encoding
			return this.objectMapper.readValue(body, targetJavaType);
		}
		String contentAsString = new String(body, encoding);
		return this.objectMapper.readValue(contentAsString, targetJavaType);
	}

	private Object convertBytesToObject(byte[] body, String encoding, Class<?> targetClass) throws IOException {
		if (this.supportedCTCharset != null) { // Jackson will determine encoding
			return this.objectMapper.readValue(body, this.objectMapper.constructType(targetClass));
		}
		String contentAsString = new String(body, encoding);
		return this.objectMapper.readValue(contentAsString, this.objectMapper.constructType(targetClass));
	}

	@Override
	protected Message createMessage(Object objectToConvert, MessageProperties messageProperties)
			throws MessageConversionException {

		return createMessage(objectToConvert, messageProperties, null);
	}

	@Override
	protected Message createMessage(Object objectToConvert, MessageProperties messageProperties,
			@Nullable Type genericType) throws MessageConversionException {

		byte[] bytes;
		try {
			if (this.charsetIsUtf8 && this.supportedCTCharset == null) {
				bytes = this.objectMapper.writeValueAsBytes(objectToConvert);
			}
			else {
				String jsonString = this.objectMapper
						.writeValueAsString(objectToConvert);
				String encoding = this.supportedCTCharset != null ? this.supportedCTCharset : getDefaultCharset();
				bytes = jsonString.getBytes(encoding);
			}
		}
		catch (IOException e) {
			throw new MessageConversionException("Failed to convert Message content", e);
		}
		messageProperties.setContentType(this.supportedContentType.toString());
		if (this.supportedCTCharset == null) {
			messageProperties.setContentEncoding(getDefaultCharset());
		}
		messageProperties.setContentLength(bytes.length);

		if (getClassMapper() == null) {
			JavaType type = this.objectMapper.constructType(
					genericType == null ? objectToConvert.getClass() : genericType);
			if (genericType != null && !type.isContainerType()
					&& Modifier.isAbstract(type.getRawClass().getModifiers())) {
				type = this.objectMapper.constructType(objectToConvert.getClass());
			}
			getJavaTypeMapper().fromJavaType(type, messageProperties);
		}
		else {
			getClassMapper().fromClass(objectToConvert.getClass(), messageProperties); // NOSONAR never null
		}

		return new Message(bytes, messageProperties);
	}

}
