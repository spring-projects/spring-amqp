/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.Channel;

/**
 * Subinterface of {@link com.rabbitmq.client.Channel} to be implemented by 
 * Channel proxies.  Allows access to the underlying target Channel
 * 
 * @author Mark Pollack
 * @see CachingConnectionFactory
 */
public interface ChannelProxy extends Channel {

	/**
	 * Return the target Channel of this proxy.
	 * <p>This will typically be the native provider Channel
	 * @return the underlying Channel (never <code>null</code>)
	 */
	Channel getTargetChannel();
}
