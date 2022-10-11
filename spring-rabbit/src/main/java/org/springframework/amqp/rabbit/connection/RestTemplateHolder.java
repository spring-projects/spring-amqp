/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import org.springframework.web.client.RestTemplate;

/**
 * Holder for a {@link RestTemplate} and credentials.
 *
 * @author Gary Russell
 * @since 2.4.8
 *
 */
class RestTemplateHolder {

	final String userName; // NOSONAR

	final String password; // NOSONAR

	RestTemplate template; // NOSONAR

	RestTemplateHolder(String userName, String password) {
		this.userName = userName;
		this.password = password;
	}

}
