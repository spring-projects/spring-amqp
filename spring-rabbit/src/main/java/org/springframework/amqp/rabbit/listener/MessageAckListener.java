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

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.lang.Nullable;

/**
 * A listener for message ack when using {@link AcknowledgeMode#AUTO}.
 *
 * @author Cao Weibo
 * @author Gary Russell
 * @since 2.4.6
 */
@FunctionalInterface
public interface MessageAckListener {

	/**
	 * Listener callback.
	 * @param success Whether ack succeed.
	 * @param deliveryTag The deliveryTag of ack.
	 * @param cause The cause of failed ack.
	 */
	void onComplete(boolean success, long deliveryTag, @Nullable Throwable cause);

}
