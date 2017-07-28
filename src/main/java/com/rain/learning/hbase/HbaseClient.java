package com.rain.learning.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseClient {
	private static final Logger logger = LoggerFactory.getLogger(HbaseClient.class);
	private Connection connection = null;
	private static HbaseClient instance = null;

	private HbaseClient() {
		init();
	}

	public void init() {
		try {
			Configuration conf = HBaseConfiguration.create();
			// zookeeper地址
			conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk3");

			conf.set("zookeeper.znode.parent", "/hbase");
			conf.set("zookeeper.recovery.retry", "3");
			conf.set("zookeeper.recovery.retry.intervalmill", "500");

			conf.set("hbase.storescanner.parallel.seek.threads", "600");
			conf.set("hbase.client.retries.number", "3");
			conf.set("hbase.client.pause", "100");

			conf.set("hbase.rpc.timeout", "3000");
			conf.set("hbase.client.operation.timeout", "60000");
			conf.set("hbase.client.scanner.timeout.period", "60000");

			// 建立连接
			connection = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			logger.error(">>>Hbase init failure:{}", e.toString());
		}
	}

	public static HbaseClient getInstance() {
		if (instance == null) {
			synchronized (HbaseClient.class) {
				if (instance == null) {
					instance = new HbaseClient();
				}
			}
		}
		return instance;
	}

	public Table getHtable(String tableName) throws IOException {
		return connection.getTable(TableName.valueOf(tableName));
	}

	public Admin getAdmin() throws IOException {
		return connection.getAdmin();
	}

	public void relaseHtable(Table table) {
		if (table == null) {
			return;
		}
		try {
			table.close();
		} catch (IOException e) {
			logger.error(">>>Error:{}", e.toString());
		}
	}

	public void destory() {
		try {
			connection.close();
		} catch (IOException e) {
			logger.error(">>>Error:{}", e.toString());
		} finally {
			instance = null;
		}
	}

}