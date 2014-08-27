/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.support;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.support.HeaderMapper;

/**
 * Strategy interface for mapping messaging Message headers to an outbound
 * {@link MessageProperties} (e.g. to configure AMQP properties) or
 * extracting messaging header values from an inbound {@link MessageProperties}.
 *
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 * @author Stephane Nicoll
 * @since 1.4
 */
public interface AmqpHeaderMapper extends HeaderMapper<MessageProperties> {
}
