package com.rain.learning.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseCreateTable {
	private static Logger logger = LoggerFactory.getLogger(HbaseCreateTable.class);

	public static void main(String[] args) {
		Admin admin = null;
		try {
			logger.info(">>>start.");
			// 表名
			String tableName = "test-hbase";
			// 列族名
			String columnFamily = "cf";

			// 表管理类
			admin = HbaseClient.getInstance().getAdmin();
			// 定义表名
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

			// 定义表结构
			HColumnDescriptor cf = new HColumnDescriptor(columnFamily);
			tableDesc.addFamily(cf);

			admin.createTable(tableDesc);

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