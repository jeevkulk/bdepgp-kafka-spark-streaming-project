package jeevkulk.rdd;

import jeevkulk.processor.DataProcessor;

public class Executor {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\installations\\apache_hadoop\\hadoop-3.1.0");
        String dataAnalysis = args[0]; //simpleMovingAverage / gainLossAverage / averageVolume
        String kafkaBrokerId = args[1]; //52.55.237.11
        String kafkaTopicName = args[2]; //stockData
        String kafkaGroupId = args[3]; //group_id_jeevkulk_1

        DataProcessor processor = new DataProcessor();
        processor.doProcess(dataAnalysis, kafkaBrokerId, kafkaTopicName, kafkaGroupId);
    }
}
