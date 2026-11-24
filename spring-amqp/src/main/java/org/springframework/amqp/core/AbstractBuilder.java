/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.core;

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Base class for builders supporting arguments.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
public abstract class AbstractBuilder {

	private @Nullable Map<String, @Nullable Object> arguments;

	/**
	 * Return the arguments map, after creating one if necessary.
	 * @return the arguments.
	 */
	protected Map<String, @Nullable Object> getOrCreateArguments() {
		if (this.arguments == null) {
			this.arguments = new LinkedHashMap<>();
		}
		return this.arguments;
	}

	protected @Nullable Map<String, @Nullable Object> getArguments() {
		return this.arguments;
	}

}
