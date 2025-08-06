package org.springframework.amqp.rabbit.listener.api;

/**
 * A functional interface to provide the number of pending replies,
 * used to delay listener container shutdown.
 *
 * @author Jeongjun Min
 * @since 4.0
 * @see org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer#setPendingReplyProvider(PendingReplyProvider)
 */
@FunctionalInterface
public interface PendingReplyProvider {

	/**
	 * Return the number of pending replies.
	 * @return the number of pending replies.
	 */
	int getPendingReplyCount();
}