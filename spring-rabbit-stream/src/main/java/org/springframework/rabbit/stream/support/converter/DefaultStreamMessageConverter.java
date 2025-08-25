/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.rabbit.stream.support.converter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.MessageBuilder.ApplicationPropertiesBuilder;
import com.rabbitmq.stream.MessageBuilder.PropertiesBuilder;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.codec.WrapperMessageBuilder;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default {@link StreamMessageConverter}.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 2.4
 *
 */
public class DefaultStreamMessageConverter implements StreamMessageConverter {

	private final Charset charset = StandardCharsets.UTF_8;

	private Supplier<MessageBuilder> builderSupplier;

	/**
	 * Construct an instance using a {@link WrapperMessageBuilder}.
	 */
	public DefaultStreamMessageConverter() {
		this.builderSupplier = WrapperMessageBuilder::new;
	}

	/**
	 * Construct an instance using the provided codec.
	 * @param codec the codec.
	 */
	public DefaultStreamMessageConverter(Codec codec) {
		this.builderSupplier = codec::messageBuilder;
	}

	/**
	 * Set a supplier for a message builder.
	 * @param builderSupplier the supplier.
	 */
	public void setBuilderSupplier(Supplier<MessageBuilder> builderSupplier) {
		this.builderSupplier = builderSupplier;
	}

	@Override
	public Message toMessage(Object object, StreamMessageProperties messageProperties) throws MessageConversionException {
		Assert.isInstanceOf(com.rabbitmq.stream.Message.class, object);
		com.rabbitmq.stream.Message streamMessage = (com.rabbitmq.stream.Message) object;
		toMessageProperties(streamMessage, messageProperties);
		return org.springframework.amqp.core.MessageBuilder.withBody(streamMessage.getBodyAsBinary())
				.andProperties(messageProperties)
				.build();
	}

	@Override
	public com.rabbitmq.stream.Message fromMessage(Message message) throws MessageConversionException {
		MessageBuilder builder = this.builderSupplier.get();
		PropertiesBuilder propsBuilder = builder.properties();
		MessageProperties props = message.getMessageProperties();
		Assert.isInstanceOf(StreamMessageProperties.class, props);
		StreamMessageProperties mProps = (StreamMessageProperties) props;
		JavaUtils.INSTANCE
				.acceptIfNotNull(mProps.getMessageId(), propsBuilder::messageId)
				.acceptIfNotNull(mProps.getUserId(), usr -> propsBuilder.userId(usr.getBytes(this.charset)))
				.acceptIfNotNull(mProps.getTo(), propsBuilder::to)
				.acceptIfNotNull(mProps.getSubject(), propsBuilder::subject)
				.acceptIfNotNull(mProps.getReplyTo(), propsBuilder::replyTo)
				.acceptIfNotNull(mProps.getCorrelationId(), propsBuilder::correlationId)
				.acceptIfNotNull(mProps.getContentType(), propsBuilder::contentType)
				.acceptIfNotNull(mProps.getContentEncoding(), propsBuilder::contentEncoding)
				.acceptIfNotNull(mProps.getCreationTime(), propsBuilder::creationTime)
				.acceptIfNotNull(mProps.getGroupId(), propsBuilder::groupId)
				.acceptIfNotNull(mProps.getGroupSequence(), propsBuilder::groupSequence)
				.acceptIfNotNull(mProps.getReplyToGroupId(), propsBuilder::replyToGroupId);
		ApplicationPropertiesBuilder appPropsBuilder = builder.applicationProperties();

		long creationTime = mProps.getCreationTime();
		if (creationTime <= 0) {
			creationTime = System.currentTimeMillis();
		}
		propsBuilder.creationTime(creationTime);

		String expiration = mProps.getExpiration();
		if (StringUtils.hasText(expiration)) {
			propsBuilder.absoluteExpiryTime(creationTime + Long.parseLong(expiration));
		}

		mProps.getHeaders().forEach((key, val) -> mapProp(key, val, appPropsBuilder));
		builder.addData(message.getBody());
		return builder.build();
	}

	private void mapProp(String key, @Nullable Object val, ApplicationPropertiesBuilder builder) {
		if (val instanceof String string) {
			builder.entry(key, string);
		}
		else if (val instanceof Long longValue) {
			builder.entry(key, longValue);
		}
		else if (val instanceof Integer intValue) {
			builder.entry(key, intValue);
		}
		else if (val instanceof Short shortValue) {
			builder.entry(key, shortValue);
		}
		else if (val instanceof Byte byteValue) {
			builder.entry(key, byteValue);
		}
		else if (val instanceof Double doubleValue) {
			builder.entry(key, doubleValue);
		}
		else if (val instanceof Float floatValue) {
			builder.entry(key, floatValue);
		}
		else if (val instanceof Character character) {
			builder.entry(key, character);
		}
		else if (val instanceof UUID uuid) {
			builder.entry(key, uuid);
		}
		else if (val instanceof byte[] bytes) {
			builder.entry(key, bytes);
		}
	}

	private void toMessageProperties(com.rabbitmq.stream.Message streamMessage,
			StreamMessageProperties mProps) {

		Properties properties = streamMessage.getProperties();
		if (properties != null) {
			JavaUtils.INSTANCE
					.acceptIfNotNull(properties.getMessageIdAsString(), mProps::setMessageId)
					.acceptIfNotNull(properties.getUserId(),
							usr -> mProps.setUserId(new String(usr, this.charset)))
					.acceptIfNotNull(properties.getTo(), mProps::setTo)
					.acceptIfNotNull(properties.getSubject(), mProps::setSubject)
					.acceptIfNotNull(properties.getReplyTo(), mProps::setReplyTo)
					.acceptIfNotNull(properties.getCorrelationIdAsString(), mProps::setCorrelationId)
					.acceptIfNotNull(properties.getContentType(), mProps::setContentType)
					.acceptIfNotNull(properties.getContentEncoding(), mProps::setContentEncoding)
					.acceptIfNotNull(properties.getGroupId(), mProps::setGroupId)
					.acceptIfNotNull(properties.getGroupSequence(), mProps::setGroupSequence)
					.acceptIfNotNull(properties.getReplyToGroupId(), mProps::setReplyToGroupId);

			long creationTime = properties.getCreationTime();
			if (creationTime <= 0) {
				creationTime = System.currentTimeMillis();
			}
			mProps.setCreationTime(creationTime);

			long absoluteExpiryTime = properties.getAbsoluteExpiryTime();
			if (absoluteExpiryTime > creationTime) {
				mProps.setExpiration(Long.toString(absoluteExpiryTime - creationTime));
			}
		}
		Map<String, Object> applicationProperties = streamMessage.getApplicationProperties();
		if (applicationProperties != null) {
			mProps.getHeaders().putAll(applicationProperties);
		}
	}

}
