/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.amqp.support.postprocessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.core.OrderComparator;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;

/**
 * Utilities for message post processors.
 *
 * @author Gary Russell
 * @since 1.4.2
 *
 */
public final class MessagePostProcessorUtils {

	public static Collection<MessagePostProcessor> sort(Collection<MessagePostProcessor> processors) {
		List<MessagePostProcessor> priorityOrdered = new ArrayList<MessagePostProcessor>();
		List<MessagePostProcessor> ordered = new ArrayList<MessagePostProcessor>();
		List<MessagePostProcessor> unOrdered = new ArrayList<MessagePostProcessor>();
		for (MessagePostProcessor processor : processors) {
			if (processor instanceof PriorityOrdered) {
				priorityOrdered.add(processor);
			}
			else if (processor instanceof Ordered) {
				ordered.add(processor);
			}
			else {
				unOrdered.add(processor);
			}
		}
		List<MessagePostProcessor> sorted = new ArrayList<MessagePostProcessor>();
		OrderComparator.sort(priorityOrdered);
		sorted.addAll(priorityOrdered);
		OrderComparator.sort(ordered);
		sorted.addAll(ordered);
		sorted.addAll(unOrdered);
		return sorted;
	}

	private MessagePostProcessorUtils() { }

}
