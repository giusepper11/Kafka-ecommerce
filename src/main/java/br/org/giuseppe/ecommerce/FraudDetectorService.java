package br.org.giuseppe.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
    public static void main(String[] args) throws InterruptedException {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER", fraudService::parseFraud)) {
            service.run();
        }
    }

    private void parseFraud(ConsumerRecord<String, String> record) throws InterruptedException {
        System.out.println("------------------------------------------");
        System.out.println("Processando nova ordem, checando fraude!!!");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(5000);
        System.out.println("Ordem processada");

    }

}
