/*
 * Copyright 2002-2019 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import org.springframework.util.StringUtils;

/**
 * A name resolver for {@link org.springframework.amqp.rabbit.core.RabbitAdmin} beans.
 *
 * @author Wander Costa
 */
final class MultiRabbitAdminNameResolver {

	private MultiRabbitAdminNameResolver() {
		throw new UnsupportedOperationException("Construction not supported.");
	}

	/**
	 * Resolves the name of the RabbitAdmin bean based on the RabbitListener.
	 *
	 * @param rabbitListener The RabbitListener to process the name from.
	 * @return The name of the RabbitAdmin bean.
	 */
	static String resolve(RabbitListener rabbitListener) {
		String admin = rabbitListener.admin();
		if (!StringUtils.hasText(admin) && StringUtils.hasText(rabbitListener.containerFactory())) {
			admin = rabbitListener.containerFactory() + MultiRabbitConstants.RABBIT_ADMIN_SUFFIX;
		}
		if (!StringUtils.hasText(admin)) {
			admin = MultiRabbitConstants.DEFAULT_RABBIT_ADMIN_BEAN_NAME;
		}
		return admin;
	}

}
