/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.amqp.rabbit.stocks.service;

import java.util.List;

import org.springframework.amqp.rabbit.stocks.domain.TradeRequest;

/**
 * Credit service to see if the incoming trade can be processed.  If it can not be processed
 * a false value is returned and the error list contains information as to what went wrong.
 * 
 * @author Mark Pollack
 *
 */
public interface CreditCheckService {

	boolean canExecute(TradeRequest tradeRequest, List<?> errors);

}
