package com.rain.learning.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseDeleteTable {
	private static Logger logger = LoggerFactory.getLogger(HbaseDeleteTable.class);

	public static void main(String[] args) {
		Admin admin = null;
		try {
			logger.info(">>>start.");
			// 表名
			String tableName = "test-hbase";

			// 表管理类
			admin = HbaseClient.getInstance().getAdmin();
			// 首先禁用表
			admin.disableTable(TableName.valueOf(tableName));
			// 然后再删除表
			admin.deleteTable(TableName.valueOf(tableName));

			logger.info(">>>end.");
		} catch (Exception e) {
			logger.error(">>>Error:{}", e.toString());
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			HbaseClient.getInstance().destory();
		}

	}

}