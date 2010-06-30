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

package org.springframework.amqp.rabbit.stocks.gateway;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.core.support.RabbitGatewaySupport;
import org.springframework.amqp.rabbit.stocks.domain.Quote;
import org.springframework.amqp.rabbit.stocks.domain.Stock;
import org.springframework.amqp.rabbit.stocks.domain.StockExchange;

/**
 * Rabbit implementation of the {@link MarketDataGateway} for sending Market data.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class RabbitMarketDataGateway extends RabbitGatewaySupport implements MarketDataGateway {

	private static Log logger = LogFactory.getLog(RabbitMarketDataGateway.class); 

	private static final Random random = new Random();

	private final List<MockStock> stocks = new ArrayList<MockStock>();


	public RabbitMarketDataGateway() {
		this.stocks.add(new MockStock("AAPL", StockExchange.nasdaq, 255));
		this.stocks.add(new MockStock("CSCO", StockExchange.nasdaq, 22));
		this.stocks.add(new MockStock("DELL", StockExchange.nasdaq, 15));
		this.stocks.add(new MockStock("GOOG", StockExchange.nasdaq, 500));
		this.stocks.add(new MockStock("INTC", StockExchange.nasdaq, 22));
		this.stocks.add(new MockStock("MSFT", StockExchange.nasdaq, 29));
		this.stocks.add(new MockStock("ORCL", StockExchange.nasdaq, 24));
		this.stocks.add(new MockStock("CAJ", StockExchange.nyse, 43));
		this.stocks.add(new MockStock("F", StockExchange.nyse, 12));
		this.stocks.add(new MockStock("GE", StockExchange.nyse, 18));
		this.stocks.add(new MockStock("HMC", StockExchange.nyse, 32));
		this.stocks.add(new MockStock("HPQ", StockExchange.nyse, 48));
		this.stocks.add(new MockStock("IBM", StockExchange.nyse, 130));
		this.stocks.add(new MockStock("TM", StockExchange.nyse, 76));
	}


	public void sendMarketData() {
		Quote quote = generateFakeQuote();
		Stock stock = quote.getStock();
		logger.info("Sending Market Data for " + stock.getTicker());
		String routingKey = "app.stock.quotes."+ stock.getStockExchange() + "." + stock.getTicker();
		getRabbitTemplate().convertAndSend(routingKey, quote);
	}

	private Quote generateFakeQuote() {
		MockStock stock = this.stocks.get(random.nextInt(this.stocks.size()));
		String price = stock.randomPrice();  
		return new Quote(stock, price);
	}


	private static class MockStock extends Stock {

		private final int basePrice;
		private final DecimalFormat twoPlacesFormat = new DecimalFormat("0.00");

		private MockStock(String ticker, StockExchange stockExchange, int basePrice) {
			this.setTicker(ticker);
			this.setStockExchange(stockExchange);
			this.basePrice = basePrice;
		}

		private String randomPrice() {
			return this.twoPlacesFormat.format(this.basePrice + Math.abs(random.nextGaussian()));
		}
	}

}
