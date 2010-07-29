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

package org.springframework.amqp.rabbit.stocks.ui;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.text.DecimalFormat;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.stocks.domain.Quote;
import org.springframework.amqp.rabbit.stocks.domain.Stock;
import org.springframework.amqp.rabbit.stocks.domain.TradeResponse;

import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

/**
 * A typical poor mans UI to drive the application.  
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
@SuppressWarnings("serial")
public class StockPanel extends JPanel {

	private static Log log = LogFactory.getLog(StockPanel.class);

	private JTextField tradeRequestTextField;
	private JButton tradeRequestButton;
	private JTextArea marketDataTextArea;
	private StockController stockController;

	private DecimalFormat frmt = new DecimalFormat("$0.00");

	public StockPanel(StockController controller) {
		this.stockController = controller;
		controller.setStockPanel(this);
		this.setBorder(BorderFactory.createTitledBorder("Stock Form"));
		
		FormLayout formLayout = new FormLayout("pref, 150dlu", // columns
				"pref, fill:100dlu:grow"); // rows
		setLayout(formLayout);
		CellConstraints c = new CellConstraints();

		tradeRequestButton = new JButton("Send Trade Request");
		add(tradeRequestButton, c.xy(1, 1));

		tradeRequestTextField = new JTextField("");
		add(tradeRequestTextField, c.xy(2, 1));

		add(new JLabel("Market Data"), c.xy(1, 2));

		marketDataTextArea = new JTextArea();
		JScrollPane sp = new JScrollPane(marketDataTextArea);
		sp.setSize(200, 300);

		add(sp, c.xy(2, 2));

		tradeRequestTextField.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent e) {
			}
			public void focusGained(FocusEvent e) {
				tradeRequestTextField.setText("");
				tradeRequestTextField.setForeground(Color.BLACK);
			}
		});

		tradeRequestButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				sendTradeRequest();
			}
		});
	}

	private void sendTradeRequest() {
		try {
			stockController.sendTradeRequest(tradeRequestTextField.getText());
			tradeRequestTextField.setForeground(Color.GRAY);
			tradeRequestTextField.setText("Request Pending...");
			log.info("Sent trade request.");
		}
		catch (Exception ex) {
			tradeRequestTextField.setForeground(Color.RED);
			tradeRequestTextField.setText("Required Format: 100 TCKR");
		}
	}

	public static void main(String[] a) {
		JFrame f = new JFrame("Rabbit Stock Demo");
		f.setDefaultCloseOperation(2);
		f.add(new StockPanel(new StockController()));
		f.pack();
		f.setVisible(true);
	}

	public void displayQuote(final Quote quote) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				Stock stock = quote.getStock();
				marketDataTextArea.append(stock.getTicker() + " " + quote.getPrice() + "\n");
			}
		});
	}

	public void update(final TradeResponse tradeResponse) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				tradeRequestTextField.setForeground(Color.GREEN);
				tradeRequestTextField.setText("Confirmed. "
						+ tradeResponse.getTicker() + " "
						+ frmt.format(tradeResponse.getPrice().doubleValue()));
			}
		});
	}

}
