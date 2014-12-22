/*
    Copyright [2014] [Iljoon Hwang, ih138@columbia.edu]
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.cijhwang.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TickDataReadWrite{
	
/*
 * Write the collected tick data to Hadoop 
 * input: 
 *     String line: tick data
 *     String loc: Hadoop file system path with file name
 */
	private static void writeTick(String line, String loc) {
		try {
			Path pt = new Path(loc);
			FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
            br.write(line);
            br.close();
		} catch (Exception e) {
			System.out.println("File not found");
		} 
	}
/*
 * Parsing the tick data from Yahoo finance
 * input: 
 *     String url: Yahoo finance tickdata url
 * otput:
 *     String res: tick data collected 
 */
	 private static String getTick(String url) throws IOException {
         URL yahoofinance = new URL(url);
         URLConnection yc = yahoofinance.openConnection();
         BufferedReader in = new BufferedReader(new InputStreamReader(
                 yc.getInputStream(), "UTF-8"));
         String inputLine;
         StringBuilder res= new StringBuilder();
         boolean start=false;
  		 // Take close price only
         while ((inputLine = in.readLine()) != null){
        	 if(inputLine.split(":")[0].equalsIgnoreCase("volume")){
        		 start = true;
        		 continue;
        	 }
        	 else if(start) {
        		 res.append(inputLine.split(",")[1]);
        		 res.append('\n');
        	 }
        	 else
        		 continue;
         }
         in.close();
         return res.toString();
     }
	 
/*
 * Collect the all target tickers 
 * output: List<String> result: List of tickers
 */	 
	public static List<String> getTickers (){
		 List<String> result=new ArrayList<String>();
		 
		 BufferedReader breader = null;
		 try {
			String line;
			breader = new BufferedReader(new FileReader("/home/hwang/bdproject/sp500"));
			while ( (line=breader.readLine()) != null) {
				result.add(line.split("\\t")[0]);
			}
		 }
		 catch (IOException e) {
			 e.printStackTrace();
		 }
		 return result;
	 }
	
/*
 * main
 * input argument: 
 *     0 : 10 days 5-minute-tick-data
 *     1 : 1 day 1-minute-tick-data
 */

	public static void main(String[] args) {
		int firstArg=0;
		String loc_pre=null;
		String loc=null;
		String url2=null;
		if (args.length > 0) {
		    try {
		        firstArg = Integer.parseInt(args[0]); } catch (NumberFormatException e) {
		        System.err.println("Argument" + args[0] + " must be an integer.");
		        System.exit(1);
		    }
		    if (firstArg == 0){
		    	loc_pre = "hdfs://master:8020/user/hwang/data/";
		    	url2 = "/chartdata;type=quote;range=10d/csv";
		    }
		    else if (firstArg == 1){
		    	loc_pre = "hdfs://master:8020/user/hwang/dailyData/";
		    	url2 = "/chartdata;type=quote;range=1d/csv";
		    }
		    else {
		        System.err.println("Argument" + args[0] + " must be an integer [0] for 10 day 5 min data or [1] for intra day 1 min data.");
		        System.exit(1);
		    }
		}
		else {
			System.out.println("Usage: hadoop jar com.cijhwang.hadoop.TickDataReadWrite [ 0: 10 day 5 min data, 1: intra day 1 min data]");
			System.exit(1);
		}
		
		// Read Ticker list
		List<String> tickers_ls =  new ArrayList<String>();
		tickers_ls = TickDataReadWrite.getTickers();
		Iterator<String> iter = tickers_ls.iterator();
		while(iter.hasNext()) {
			String url1 = "http://chartapi.finance.yahoo.com/instrument/1.0/";
			String ticker = (String) iter.next();
			String url = url1 + ticker + url2;
			String res=null;
			try {
				res = TickDataReadWrite.getTick(url);
			} catch (IOException e) {
				e.printStackTrace();
			}
		// Write daily tick data to hadoop
			loc = loc_pre + ticker;
			try {
				TickDataReadWrite.writeTick(res, loc);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				continue;
			}
		}
	}
}
