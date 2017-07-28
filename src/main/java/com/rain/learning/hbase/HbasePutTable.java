package com.rain.learning.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbasePutTable {
	private static Logger logger = LoggerFactory.getLogger(HbasePutTable.class);

	public static void main(String[] args) {
		try {
			logger.info(">>>start.");
			// 表名
			String tableName = "test-hbase";
			// 列族名
			String columnFamily = "cf";

			String qualifier = "b";
			String rowkey = "400";
			String value = "100000";

			Table table = HbaseClient.getInstance().getHtable(tableName);

			Put put = new Put(rowkey.getBytes());
			put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
			table.put(put);

			table.close();

			logger.info(">>>end.");
		} catch (Exception e) {
			logger.error(">>>Error:{}", e.toString());
		} finally {
			HbaseClient.getInstance().destory();
		}
	}

}