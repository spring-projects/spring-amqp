/*
 * Copyright 2021 the original author or authors.
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

import java.util.Map;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.util.Assert;

import com.rabbitmq.stream.MessageBuilder.PropertiesBuilder;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.codec.WrapperMessageBuilder;

/**
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamMessageConverter implements MessageConverter {

	@Override
	public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
		Assert.isInstanceOf(com.rabbitmq.stream.Message.class, object);
		com.rabbitmq.stream.Message qpid = (com.rabbitmq.stream.Message) object;
		toMessageProperties(qpid, messageProperties);
		return MessageBuilder.withBody(qpid.getBodyAsBinary()).andProperties(messageProperties).build();
	}

	@Override
	public com.rabbitmq.stream.Message fromMessage(Message message) throws MessageConversionException {
		WrapperMessageBuilder builder = new WrapperMessageBuilder();
		PropertiesBuilder propsBuilder = builder.properties();
		MessageProperties mProps = message.getMessageProperties();
		JavaUtils.INSTANCE
				.acceptIfNotNull(mProps.getMessageId(), propsBuilder::messageId);
		// TODO ...
		builder.addData(message.getBody());
		return builder.build();
	}

	private void toMessageProperties(com.rabbitmq.stream.Message qpid, MessageProperties messageProperties) {
		Properties properties = qpid.getProperties();
		JavaUtils.INSTANCE
				.acceptIfNotNull(properties.getMessageIdAsString(), messageProperties::setMessageId);
		// TODO ...
		Map<String, Object> applicationProperties = qpid.getApplicationProperties();
		if (applicationProperties != null) {
			messageProperties.getHeaders().putAll(applicationProperties);
		}
	}

}