/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
