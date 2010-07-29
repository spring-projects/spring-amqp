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
package org.springframework.amqp.rabbit.stocks.service.stubs;

import java.util.List;

import org.springframework.amqp.rabbit.stocks.domain.TradeRequest;
import org.springframework.amqp.rabbit.stocks.service.CreditCheckService;

/***
 * An implementation that always returns true (just like real-life for a mortgage check :)
 * 
 * @author Mark Pollack
 *
 */
public class CreditCheckServiceStub implements CreditCheckService {

	public boolean canExecute(TradeRequest tradeRequest, List<?> errors) {
		return true;
	}

}
