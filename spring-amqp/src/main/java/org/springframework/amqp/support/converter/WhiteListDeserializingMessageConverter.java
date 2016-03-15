/*
 * Copyright 2016 the original author or authors.
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.util.PatternMatchUtils;

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

	protected void checkWhiteList(Class<?> clazz) throws IOException {
		if (this.whiteListPatterns.isEmpty()) {
			return;
		}
		if (clazz.isArray() || clazz.isPrimitive() || clazz.equals(String.class)
				|| Number.class.isAssignableFrom(clazz)) {
			return;
		}
		String className = clazz.getName();
		for (String pattern : this.whiteListPatterns) {
			if (PatternMatchUtils.simpleMatch(pattern, className)) {
				return;
			}
		}
		throw new SecurityException("Attempt to deserialize unauthorized " + clazz);
	}

}
