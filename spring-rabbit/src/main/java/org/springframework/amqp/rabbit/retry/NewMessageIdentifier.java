/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import org.springframework.amqp.core.Message;

/**
 * An optimization for stateful retry of message processing. If a message is known to be "new", i.e. never consumed
 * before by this or any other client, then there are potential optimizations for managing the state associated with
 * tracking the processing of a message (e.g. there is no need to check a cache for a hit).
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
@FunctionalInterface
public interface NewMessageIdentifier {

	/**
	 * Query a message to see if it has been seen before. Usually it is only possible to know if it has definitely not
	 * been seen before (e.g. through the redelivered flag, which would be used by default). Clients can customize the
	 * retry behaviour for failed messages by implementing this method.
	 *
	 * @param message the message to test
	 * @return true if the message is known to not have been consumed before
	 */
	boolean isNew(Message message);

}
