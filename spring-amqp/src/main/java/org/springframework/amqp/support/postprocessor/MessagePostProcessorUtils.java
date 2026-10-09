/*
 * Copyright 2014-present the original author or authors.
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
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 1.4.2
 *
 */
public final class MessagePostProcessorUtils {

	public static Collection<MessagePostProcessor> sort(Collection<MessagePostProcessor> processors) {
		int potentialSize = processors.size();
		List<MessagePostProcessor> priorityOrdered = new ArrayList<>(potentialSize);
		List<MessagePostProcessor> ordered = new ArrayList<>(potentialSize);
		List<MessagePostProcessor> unOrdered = new ArrayList<>(potentialSize);
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
		OrderComparator.sort(priorityOrdered);
		List<MessagePostProcessor> sorted = new ArrayList<>(priorityOrdered);
		OrderComparator.sort(ordered);
		sorted.addAll(ordered);
		sorted.addAll(unOrdered);
		return sorted;
	}

	private MessagePostProcessorUtils() { }

}
