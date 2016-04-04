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

package org.springframework.amqp.rabbit.support;

import org.springframework.amqp.core.MessageProperties;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

/**
 * Strategy interface for converting between Spring AMQP {@link MessageProperties}
 * and RabbitMQ BasicProperties.
 *
 * @author Mark Fisher
 * @since 1.0
 */
public interface MessagePropertiesConverter {

	MessageProperties toMessageProperties(BasicProperties source, Envelope envelope, String charset);

	BasicProperties fromMessageProperties(MessageProperties source, String charset);

}
