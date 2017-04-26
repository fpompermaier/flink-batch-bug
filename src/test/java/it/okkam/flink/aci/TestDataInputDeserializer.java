package it.okkam.flink.aci;

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestDataInputDeserializer {

	@Test
	@SuppressWarnings("serial")
	public void test() throws Exception {
		ExecutionEnvironment env = getExecutionEnv();

		List<Row> rowList = generateSampleRows();
		DataSet<Row> rows = env.fromCollection(rowList, new RowTypeInfo(ft))
				.flatMap(new FlatMapFunction<Row, Row>() {

					@Override
					public void flatMap(Row row, Collector<Row> out) throws Exception {
						// multiply test rows
						for (int i = 0; i < 100; i++) {
							row.setField(0, generateLongString());
							out.collect(row);
						}
					}
				}).returns(new RowTypeInfo(ft))
				.rebalance()
				.groupBy(x -> (Long) x.getField(15)) //
				.reduceGroup(new RichGroupReduceFunction<Row, Row>() {

					@Override
					public void reduce(Iterable<Row> it, Collector<Row> out) throws Exception {
						final UUID uuid = UUID.randomUUID();
						for (Row row : it) {
							row.setField(1, "http://" + uuid);
							out.collect(row);
						}
					}

				});
		rows.print();
	}

	private TypeInformation<?>[] ft = new TypeInformation<?>[] { BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO };
			
	private static String generateLongString() {
		final StringBuilder str = new StringBuilder(
				"AAAAAAAAAAAAAAAAAAAA AAAAAAAAAAAAAAAAAAAAAAAAAAA  " + ":AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA::?");
		for (int j = 0; j < 12; j++) {
			str.append(str);
		}
		final String longString = str.toString();
		return longString;
	}
	
	private List<Row> generateSampleRows() {
		final int NUM_RECORDS = 100;
		List<Row> rowList = new ArrayList<>();
		for (int i = 0; i < NUM_RECORDS; i++) {
			Row row = new Row(ft.length);
			for (int j = 0; j < ft.length; j++) {
				if (ft[j].getTypeClass().equals(String.class)) {
					if (i % 2 == 0) {
						//sometimes set a null value
						row.setField(j, null);
					} else {
						StringBuilder longString = new StringBuilder("Test " + j);
						for (int k = 0; k < 15; k++) {
							longString.append(longString);
						}
						row.setField(j, longString.toString());
					}
				} else if (ft[j].getTypeClass().equals(Integer.class)) {
					row.setField(j, new Integer(j));
				} else if (ft[j].getTypeClass().equals(Long.class)) {
					row.setField(j, new Long(j * 2));
				} else if (ft[j].getTypeClass().equals(Boolean.class)) {
					row.setField(j, i % 2 == 0);
				} else {
					throw new IllegalArgumentException("Not handled type: " + ft[j]);
				}
			}
			rowList.add(row);
		}
		return rowList;
	}

	protected static ExecutionEnvironment getExecutionEnv() {
		// SLF4JBridgeHandler.install();// Parquet use java.util.logging (jul)
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if (env instanceof LocalEnvironment) {
			Configuration conf = new Configuration();
			conf.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, "/tmp/flink");
			conf.setString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, "/tmp/flink");
			conf.setLong(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 2048 * 2);
			conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
			conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
			env = ExecutionEnvironment.createLocalEnvironment(conf);
			env.setParallelism(2);
		}
		env.registerTypeWithKryoSerializer(DateTime.class, JodaDateTimeSerializer.class);
		return env;
	}
}
