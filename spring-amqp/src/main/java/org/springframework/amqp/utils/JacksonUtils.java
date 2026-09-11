/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.utils;

import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.MimeType;

/**
 * Utilities for working with JSON {@link MimeType}s.
 *
 * @author Ngoc Nhan
 *
 * @since 4.2
 */
public final class JacksonUtils {

	/**
	 * Wildcard for {@code application/*+json}.
	 */
	public static final MimeType APPLICATION_JSON_WILDCARD = MimeType.valueOf("application/*+json");

	/**
	 * Wildcards supported for JSON content.
	 */
	public static Set<MimeType> JSON_WILDCARDS = Set.of(APPLICATION_JSON_WILDCARD,
			MimeType.valueOf(MessageProperties.CONTENT_TYPE_JSON_ALT));

	private JacksonUtils() {
	}

	/**
	 * Determine whether the given mime type is supported as JSON content.
	 * @param mimeType the mime type to check (can be null)
	 * @return {@code true} if the {@code mimeType} is compatible with a supported JSON type
	 */
	public static boolean isJsonSupported(@Nullable MimeType mimeType) {

		if (mimeType == null) {
			return false;
		}

		for (MimeType jsonMimeType : JSON_WILDCARDS) {
			if (jsonMimeType.isCompatibleWith(mimeType)) {
				return true;
			}
		}

		return false;
	}

}
