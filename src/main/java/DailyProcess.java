import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

public class DailyProcess {
	static Connection connection;

	private static Connection createConnection() throws Exception {
		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?user=grok&password=grok&ssl=false");
	}

	public static void exportDailyDeviceUsage(SparkSession spark) {
		long currentDate = new DateTime().withTimeAtStartOfDay().getMillis() / 1000;
		long hourBefore = currentDate - 24 * 3600;
		String query = "select device_family, count(*) from events WHERE created_at >= " + hourBefore
				+ " AND created_at <= " + currentDate + " group by device_family";
		System.out.println(query);
		Dataset<Row> uuids = spark.sql(query);

		JavaRDD<Row> rdd = uuids.javaRDD();
		JavaRDD<String> rddStr = rdd.map(new Function<Row, String>() {

			@Override
			public String call(Row row) throws Exception {
				System.out.println("'" + (String) row.get(0) + "'");
				return "'" + (String) row.get(0) + "','" + (String) row.get(1) + "'";
			}
		});

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String date = sdf.format(new Date(currentDate * 1000));
		Iterator<String> iterator = rddStr.toLocalIterator();
		StringBuilder sb = new StringBuilder();
		while (iterator.hasNext()) {
			String n = (String) iterator.next();
			if (sb.length() > 0)
				sb.append(',');
			sb.append("(").append(n).append(",'").append(date).append("')");

		}

		String values = sb.toString();
		if (values.length() == 0) {
			return;
		}
		try {
			connection.createStatement();
			String q = String.format("INSERT INTO device_usages(device_family, time_usage,  created_at) VALUES %s", values);
			System.out.println(q);
			PreparedStatement stm = connection.prepareStatement(q);
			stm.execute();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			connection = createConnection();
			connection.setAutoCommit(false);
		} catch (Exception e) {
			e.printStackTrace();
		}

		SparkSession spark = SparkSession.builder().appName("ProcessingData")
				.config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport().getOrCreate();

		spark.sql("show tables").show();

		exportDailyDeviceUsage(spark);

	}
}
