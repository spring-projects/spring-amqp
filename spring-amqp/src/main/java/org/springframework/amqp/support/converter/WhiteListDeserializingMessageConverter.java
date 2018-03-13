/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.amqp.utils.SerializationUtils;

/**
 * MessageConverters that potentially use Java deserialization.
 *
 * @author Gary Russell
 * @since 1.5.5
 *
 */
public abstract class WhiteListDeserializingMessageConverter extends AbstractMessageConverter {

	private final Set<String> whiteListPatterns = new LinkedHashSet<String>();

	/**
	 * Set simple patterns for allowable packages/classes for deserialization.
	 * The patterns will be applied in order until a match is found.
	 * A class can be fully qualified or a wildcard '*' is allowed at the
	 * beginning or end of the class name.
	 * Examples: {@code com.foo.*}, {@code *.MyClass}.
	 * @param whiteListPatterns the patterns.
	 */
	public void setWhiteListPatterns(List<String> whiteListPatterns) {
		this.whiteListPatterns.clear();
		this.whiteListPatterns.addAll(whiteListPatterns);
	}

	/**
	 * Add package/class patterns to the white list.
	 * @param patterns the patterns to add.
	 * @since 1.5.7
	 * @see #setWhiteListPatterns(List)
	 */
	public void addWhiteListPatterns(String... patterns) {
		Collections.addAll(this.whiteListPatterns, patterns);
	}

	protected void checkWhiteList(Class<?> clazz) throws IOException {
		SerializationUtils.checkWhiteList(clazz, this.whiteListPatterns);
	}

}
