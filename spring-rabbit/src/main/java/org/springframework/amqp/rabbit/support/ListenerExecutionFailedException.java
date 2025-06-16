/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;


/**
 * Exception to be thrown when the execution of a listener method failed.
 *
 * @author Juergen Hoeller
 * @author Gary Russell
 *
 * @see org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter
 */
@SuppressWarnings("serial")
public class ListenerExecutionFailedException extends AmqpException {

	private final List<Message> failedMessages = new ArrayList<>();

	/**
	 * Constructor for ListenerExecutionFailedException.
	 * @param msg the detail message
	 * @param cause the exception thrown by the listener method
	 * @param failedMessage the message(s) that failed
	 */
	public ListenerExecutionFailedException(String msg, Throwable cause, Message... failedMessage) {
		super(msg, cause);
		this.failedMessages.addAll(Arrays.asList(failedMessage));
	}

	public Message getFailedMessage() {
		return this.failedMessages.get(0);
	}

	public Collection<Message> getFailedMessages() {
		return Collections.unmodifiableList(this.failedMessages);
	}

}
