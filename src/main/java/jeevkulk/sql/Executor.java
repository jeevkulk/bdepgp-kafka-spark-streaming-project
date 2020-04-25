package jeevkulk.sql;

import com.google.gson.Gson;

public class Executor {

    final static Gson gson = new Gson();
    public static void main(String[] args) throws Exception {
        /*System.setProperty("hadoop.home.dir", "E:\\installations\\apache_hadoop\\hadoop-3.1.0");
        SparkSession sparkSession = SparkSession.builder().appName("StockMarketAnalysis").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        Dataset<Row> inputDF = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","52.55.237.11:9092")
                .option("subscribe","stockData")
                .load();
        inputDF.show();

        Dataset<String> symbolDS = inputDF.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

        Dataset<Row> instrumentDS = symbolDS
                .map((MapFunction<String, Instrument>) jsonString -> gson.fromJson(jsonString, Instrument.class), Encoders.bean(Instrument.class))
                .map((MapFunction<Instrument, FlattenedInstrument>) instrument -> new FlattenedInstrument(instrument), Encoders.bean(FlattenedInstrument.class))
                .selectExpr("symbol","volume","eventTime");
        //instrumentDS.show();
        Dataset<Row> volumeDF = instrumentDS.groupBy(window(col("eventTime"),"2 minutes"), col("symbol")).agg(sum("volume"));

        StreamingQuery streamingQuery = volumeDF.writeStream()
                .option("checkpointLocation","/user/ec2-user/checkpointing")
                .option("truncate", false)
                .outputMode(OutputMode.Update())
                .format("console")
                .start();
        streamingQuery.awaitTermination();*/
    }
}
