/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.listener.adapter;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;

/**
 * Root object for reply expression evaluation.
 *
 * @param request the request message.
 * @param source the source data (e.g. {@code o.s.messaging.Message<?>}).
 * @param result the result.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public record ReplyExpressionRoot(Message request, @Nullable Object source, @Nullable Object result) {

}
