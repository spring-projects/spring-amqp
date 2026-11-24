/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.rabbit.stream.listener;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler.Context;

import org.springframework.amqp.core.MessageListener;

/**
 * A message listener that receives native stream messages.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public interface StreamMessageListener extends MessageListener {

	/**
	 * Process a message.
	 * @param message the message.
	 * @param context the stream context.
	 */
	void onStreamMessage(Message message, Context context);

}
