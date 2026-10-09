/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.rabbit.retry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.Message;

/**
 * The {@link MessageRecoverer} implementation to throw an {@link ImmediateRequeueAmqpException}
 * for subsequent requeuing in the listener container.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 2.1
 */
public class ImmediateRequeueMessageRecoverer implements MessageRecoverer {

	protected Log logger = LogFactory.getLog(ImmediateRequeueMessageRecoverer.class); // NOSONAR protected

	@Override
	public void recover(Message message, @Nullable Throwable cause) {
		if (this.logger.isWarnEnabled()) {
			this.logger.warn("Retries exhausted for message " + message + "; requeuing...", cause);
		}
		if (cause != null) {
			throw new ImmediateRequeueAmqpException(cause);
		}
		else {
			throw new ImmediateRequeueAmqpException("Re-queueing for message: " + message);
		}
	}

}
