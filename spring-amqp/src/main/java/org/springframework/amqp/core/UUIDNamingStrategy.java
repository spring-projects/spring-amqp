/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.amqp.core;

import java.util.UUID;

/**
 * Generates names using {@link UUID#randomUUID()}. (e.g.
 * "f20c818a-006b-4416-bf91-643590fedb0e").
 *
 * @author Gary Russell
 *
 * @since 2.1
 */
public class UUIDNamingStrategy implements NamingStrategy {

	/**
	 * The default instance.
	 */
	public static final UUIDNamingStrategy DEFAULT = new UUIDNamingStrategy();

	@Override
	public String generateName() {
		return UUID.randomUUID().toString();
	}

}
