package com.afjcjsbx.stock;

import yahoofinance.Stock;
import yahoofinance.YahooFinance;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class DataServer {

	public static void main(String[] args) throws IOException{

		ServerSocket listener = new ServerSocket(9090);
		try{
				Socket socket = listener.accept();
				System.out.println("Got new connection: " + socket.toString());
				try {
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

					final String SYMBOL = "FTNT";
					//Date d = new Date();
					while (true){
						Stock stock = YahooFinance.get(SYMBOL);

						BigDecimal price = stock.getQuote().getPrice();
						String payload = "" + SYMBOL + "," + System.currentTimeMillis() + "," + price;

						System.out.println(price);
						out.println(payload);
						Thread.sleep(100);
					}

				} finally{
					socket.close();
				}

		} catch(Exception e ){
			e.printStackTrace();
		} finally{
			listener.close();
		}
	}
}

