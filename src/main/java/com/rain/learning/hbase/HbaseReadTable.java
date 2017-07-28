package com.rain.learning.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseReadTable {
	private static Logger logger = LoggerFactory.getLogger(HbaseReadTable.class);

	public static void main(String[] args) {
		try {
			logger.info(">>>start.");
			// 表名
			String tableName = "test-hbase";
			// 列族名
			String columnFamily = "cf";

			String qualifier = "a";
			String rowkey = "100";

			Table table = HbaseClient.getInstance().getHtable(tableName);

			Get get = new Get(rowkey.getBytes());
			get.addColumn(columnFamily.getBytes(), qualifier.getBytes());
			Result result = table.get(get);

			String resultStr = Bytes.toString(result.getValue(columnFamily.getBytes(), qualifier.getBytes()));

			logger.info(">>>Result:{}", resultStr);

			logger.info(">>>end.");
		} catch (Exception e) {
			logger.error(">>>Error:{}", e.toString());
		} finally {
			HbaseClient.getInstance().destory();
		}

	}

}