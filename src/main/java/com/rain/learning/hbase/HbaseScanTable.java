package com.rain.learning.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseScanTable {
	private static Logger logger = LoggerFactory.getLogger(HbaseScanTable.class);

	public static void main(String[] args) {
		try {
			logger.info(">>>start.");
			// 表名
			String tableName = "test-hbase";
			// 列族名
			String columnFamily = "cf";

			String qualifier = "a";
			String startRow = "100";
			String endRow = "300";

			Table table = HbaseClient.getInstance().getHtable(tableName);

			Scan scan = new Scan();
			scan.setStartRow(startRow.getBytes());
			scan.setStopRow(endRow.getBytes());

			scan.addColumn(columnFamily.getBytes(), qualifier.getBytes());

			String resultStr = "";
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
				resultStr = Bytes.toString(result.getValue(columnFamily.getBytes(), qualifier.getBytes()));
				logger.info(">>>Result:{}", resultStr);
			}

			table.close();
			logger.info(">>>end.");
		} catch (Exception e) {
			logger.error(">>>Error:{}", e.toString());
		} finally {
			HbaseClient.getInstance().destory();
		}

	}

}