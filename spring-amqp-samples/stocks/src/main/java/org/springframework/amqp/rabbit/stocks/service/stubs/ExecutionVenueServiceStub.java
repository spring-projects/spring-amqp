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

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.stocks.domain.TradeRequest;
import org.springframework.amqp.rabbit.stocks.domain.TradeResponse;
import org.springframework.amqp.rabbit.stocks.service.ExecutionVenueService;

/**
 * Execute the trade, setting the execution price to changing value in line with what the market data feed is producing.
 * 
 * @author Mark Pollack
 *
 */
public class ExecutionVenueServiceStub implements ExecutionVenueService {

	private static Log log = LogFactory.getLog(ExecutionVenueServiceStub.class);
	
	private Random random = new Random();
	
	public TradeResponse executeTradeRequest(TradeRequest request) {
		TradeResponse response = new TradeResponse();
		response.setOrderType(request.getOrderType());
		response.setPrice(calculatePrice(request.getTicker(), request.getQuantity(), request.getOrderType(), request.getPrice(), request.getUserName()));
		response.setQuantity(request.getQuantity());
		response.setTicker(request.getTicker());
		response.setConfirmationNumber(UUID.randomUUID().toString());
		
		
		try {
			log.info("Sleeping 2 seconds to simulate processing..");
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			log.error("Didn't finish sleeping", e);
		}
		return response;
	}

	private BigDecimal calculatePrice(String ticker, long quantity,
			String orderType, BigDecimal price, String userName) {
        // provide as sophisticated implementation...for now all the same price.
        if (orderType.compareTo("LIMIT") == 0)
        {
            return price;
        }
        else
        {
        	//in line with market data implementation
            return new BigDecimal(22 + Math.abs(gaussian()));
        }        
	}
	
	private double gaussian() {
		return random.nextGaussian();
	}	

}
