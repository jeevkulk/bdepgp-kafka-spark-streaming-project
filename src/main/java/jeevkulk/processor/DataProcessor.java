package jeevkulk.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import jeevkulk.domain.FlattenedInstrument;
import jeevkulk.domain.Instrument;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DataProcessor implements Serializable {

    public void doProcess(String dataAnalysis, String kafkaBrokerId, String kafkaTopicName, String kafkaGroupId) {
        Gson gson = new Gson();
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaBrokerId+":9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaGroupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(kafkaTopicName);

        SparkConf conf = new SparkConf().setAppName("Kafka_SparkStreaming").setMaster("local[*]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.minutes(1));
        //javaStreamingContext.checkpoint("E:\\checkpointing");
        javaStreamingContext.sparkContext().setLogLevel("WARN");

        JavaInputDStream<ConsumerRecord<String, String>> streamData = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        /*JavaDStream<Instrument> javaDStreamData = streamData.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, Instrument>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<Instrument> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                TypeReference<List<Instrument>> mapType = new TypeReference<List<Instrument>>() {};
                List<Instrument> instrumentList = mapper.readValue(consumerRecord.value(), mapType);
                return instrumentList.iterator();
            }
        });*/
        JavaPairDStream<String, FlattenedInstrument> javaPairDStreamData = streamData.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, FlattenedInstrument>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, FlattenedInstrument> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                TypeReference<Instrument> mapType = new TypeReference<Instrument>() {
                };
                Instrument instrument = mapper.readValue(consumerRecord.value(), mapType);
                return new Tuple2(instrument.getSymbol(), new FlattenedInstrument(instrument));
            }
        });
        Function2 aggregateFlattenedInstrumentFunction = new Function2<FlattenedInstrument, FlattenedInstrument, FlattenedInstrument>() {
            @Override
            public FlattenedInstrument call(FlattenedInstrument flattenedInstrument1, FlattenedInstrument flattenedInstrument2) throws Exception {
                FlattenedInstrument flattenedInstrument = new FlattenedInstrument();
                flattenedInstrument.setSymbol(flattenedInstrument1.getSymbol());
                flattenedInstrument.setOpen(flattenedInstrument1.getOpen() + flattenedInstrument2.getOpen());
                flattenedInstrument.setClose(flattenedInstrument1.getClose() + flattenedInstrument2.getClose());
                flattenedInstrument.setVolume(Math.abs(flattenedInstrument1.getVolume()) + Math.abs(flattenedInstrument2.getVolume()));
                flattenedInstrument.setDataPointsCount(flattenedInstrument1.getDataPointsCount() + 1);
                return flattenedInstrument;
            }
        };
        /*Function2 inverseFlattenedInstrumentFunction = new Function2<FlattenedInstrument, FlattenedInstrument, FlattenedInstrument>() {
            @Override
            public FlattenedInstrument call(FlattenedInstrument flattenedInstrument1, FlattenedInstrument flattenedInstrument2) throws Exception {
                FlattenedInstrument flattenedInstrument = new FlattenedInstrument();
                flattenedInstrument.setSymbol(flattenedInstrument1.getSymbol());
                flattenedInstrument.setOpen(flattenedInstrument1.getOpen() - flattenedInstrument2.getOpen());
                flattenedInstrument.setClose(flattenedInstrument1.getClose() - flattenedInstrument2.getClose());
                flattenedInstrument.setVolume(Math.abs(flattenedInstrument1.getVolume()) - Math.abs(flattenedInstrument2.getVolume()));
                return flattenedInstrument;
            }
        };*/

        if ("simpleMovingAverage".equals(dataAnalysis)) {
            JavaPairDStream<String, FlattenedInstrument> javaPairDStreamData1 = javaPairDStreamData.reduceByKeyAndWindow(
                    aggregateFlattenedInstrumentFunction,
                    //inverseFlattenedInstrumentFunction,
                    Durations.minutes(10),
                    Durations.minutes(5)
            );
            calculateSimpleMovingAverage(javaPairDStreamData1);
        } else if ("gainLossAverage".equals(dataAnalysis)) {
            JavaPairDStream<String, FlattenedInstrument> javaPairDStreamData1 = javaPairDStreamData.reduceByKeyAndWindow(
                    aggregateFlattenedInstrumentFunction,
                    //inverseFlattenedInstrumentFunction,
                    Durations.minutes(10),
                    Durations.minutes(5)
            );
            calculateGainLossAverage(javaPairDStreamData1);
        } else if ("averageVolume".equals(dataAnalysis)) {
            JavaPairDStream<String, FlattenedInstrument> javaPairDStreamData2 = javaPairDStreamData.reduceByKeyAndWindow(
                    aggregateFlattenedInstrumentFunction,
                    //inverseFlattenedInstrumentFunction,
                    Durations.minutes(10),
                    Durations.minutes(10)
            );
            calculateVolumes(javaPairDStreamData2);
        }
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void calculateSimpleMovingAverage(JavaPairDStream<String, FlattenedInstrument> javaPairDStreamData) {
        JavaPairDStream<String, Double> javaPairDStreamSimpleMovingAverages = javaPairDStreamData.mapToPair(new PairFunction<Tuple2<String, FlattenedInstrument>, String, Double>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Double> call(Tuple2<String, FlattenedInstrument> flattenedInstrumentTuple) throws Exception {
                FlattenedInstrument flattenedInstrument = flattenedInstrumentTuple._2;
                String symbol = flattenedInstrument.getSymbol();
                double closingPrice = flattenedInstrument.getClose();
                int dataPointsCount = flattenedInstrument.getDataPointsCount();
                double simpleMovingAverage = 0.0;
                if (dataPointsCount > 0)
                    simpleMovingAverage = closingPrice/dataPointsCount;
                return new Tuple2<String, Double>(symbol, simpleMovingAverage);
            }
        });
        javaPairDStreamSimpleMovingAverages.map(tuple2 -> "SimpleMovingAverage for last 5 mins window ==>" + tuple2._1 + " = " + tuple2._2.toString()).print();
    }

    private void calculateGainLossAverage(JavaPairDStream<String, FlattenedInstrument> javaPairDStreamData) {
        JavaPairDStream<String, Double> javaPairDStreamSimpleMovingAverages = javaPairDStreamData.mapToPair(new PairFunction<Tuple2<String, FlattenedInstrument>, String, Double>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Double> call(Tuple2<String, FlattenedInstrument> flattenedInstrumentTuple) throws Exception {
                FlattenedInstrument flattenedInstrument = flattenedInstrumentTuple._2;
                String symbol = flattenedInstrument.getSymbol();
                double openingPrice = flattenedInstrument.getOpen();
                double closingPrice = flattenedInstrument.getClose();
                double gainLoss = closingPrice - openingPrice;
                int dataPointsCount = flattenedInstrument.getDataPointsCount();
                return new Tuple2<String, Double>(symbol, gainLoss / dataPointsCount);
            }
        });
        javaPairDStreamSimpleMovingAverages.map(tuple2 -> "GainLossAverage for last 5 mins window ==>" + tuple2._1 + " = " + tuple2._2.toString()).print();
    }

    private void calculateVolumes(JavaPairDStream<String, FlattenedInstrument> javaPairDStreamData) {
        JavaPairDStream<String, Double> javaPairDStreamSimpleMovingAverages = javaPairDStreamData.mapToPair(new PairFunction<Tuple2<String, FlattenedInstrument>, String, Double>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Double> call(Tuple2<String, FlattenedInstrument> flattenedInstrumentTuple) throws Exception {
                FlattenedInstrument flattenedInstrument = flattenedInstrumentTuple._2;
                String symbol = flattenedInstrument.getSymbol();
                double totalVolume = flattenedInstrument.getVolume();
                return new Tuple2<String, Double>(symbol, totalVolume);
            }
        });
        javaPairDStreamSimpleMovingAverages.map(tuple2 -> "Volume for last 10 mins window ==>" + tuple2._1 + " = " + tuple2._2.toString()).print();
    }
}
