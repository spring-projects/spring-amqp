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

package org.springframework.amqp.rabbit.stocks.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.stocks.domain.Quote;
import org.springframework.amqp.rabbit.stocks.domain.Stock;
import org.springframework.amqp.rabbit.stocks.domain.TradeResponse;
import org.springframework.amqp.rabbit.stocks.ui.StockController;

/**
 * POJO handler that receives market data and trade responses.  Calls are delegated to the UI controller.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class ClientHandler {

	private static Log log = LogFactory.getLog(ClientHandler.class);

	private StockController stockController;

	public StockController getStockController() {
		return stockController;
	}

	public void setStockController(StockController stockController) {
		this.stockController = stockController;
	}

	public void handleMessage(Quote quote) {
		Stock stock = quote.getStock();
		log.info("Received market data.  Ticker = " + stock.getTicker() + ", Price = " + quote.getPrice());
		stockController.displayQuote(quote);
	}

	public void handleMessage(TradeResponse tradeResponse) {
		log.info("Received trade repsonse. [" + tradeResponse + "]");
		stockController.UpdateTrade(tradeResponse);
	}

}
