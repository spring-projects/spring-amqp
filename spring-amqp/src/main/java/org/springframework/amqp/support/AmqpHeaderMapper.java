/*
 * Copyright 2002-present the original author or authors.
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
