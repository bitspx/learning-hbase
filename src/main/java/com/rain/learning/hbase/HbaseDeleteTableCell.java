package com.rain.learning.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseDeleteTableCell {
	private static Logger logger = LoggerFactory.getLogger(HbaseDeleteTableCell.class);

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
			// 实例化Delete实例
			Delete delete = new Delete(rowkey.getBytes());
			// 添加删除条件
			delete.addColumn(columnFamily.getBytes(), qualifier.getBytes());
			table.delete(delete);

			table.close();

			logger.info(">>>end.");
		} catch (Exception e) {
			logger.error(">>>Error:{}", e.toString());
		} finally {
			HbaseClient.getInstance().destory();
		}

	}

}