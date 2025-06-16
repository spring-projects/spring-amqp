/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.amqp.utils.SerializationUtils;

/**
 * MessageConverters that potentially use Java deserialization.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 1.5.5
 *
 */
public abstract class AllowedListDeserializingMessageConverter extends AbstractMessageConverter {

	private final Set<String> allowedListPatterns = new LinkedHashSet<>();

	/**
	 * Set simple patterns for allowable packages/classes for deserialization.
	 * The patterns will be applied in order until a match is found.
	 * A class can be fully qualified or a wildcard '*' is allowed at the
	 * beginning or end of the class name.
	 * Examples: {@code com.foo.*}, {@code *.MyClass}.
	 * @param patterns the patterns.
	 */
	public void setAllowedListPatterns(List<String> patterns) {
		this.allowedListPatterns.clear();
		this.allowedListPatterns.addAll(patterns);
	}

	/**
	 * Add package/class patterns to the allowed list.
	 * @param patterns the patterns to add.
	 * @since 1.5.7
	 * @see #setAllowedListPatterns(List)
	 */
	public void addAllowedListPatterns(String... patterns) {
		Collections.addAll(this.allowedListPatterns, patterns);
	}

	protected void checkAllowedList(Class<?> clazz) {
		SerializationUtils.checkAllowedList(clazz, this.allowedListPatterns);
	}

}
