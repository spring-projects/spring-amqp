/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

/**
 * Extension of the {@link DefaultMessageHandlerMethodFactory} for Spring AMQP requirements.
 *
 * @author Artem Bilan
 *
 * @since 3.0.5
 *
 * @deprecated since 4.1 in favor of Spring AMQP's {@link org.springframework.amqp.listener.adapter.AmqpMessageHandlerMethodFactory}.
 */
@Deprecated(forRemoval = true, since = "4.1")
public class AmqpMessageHandlerMethodFactory extends org.springframework.amqp.listener.adapter.AmqpMessageHandlerMethodFactory {

}
