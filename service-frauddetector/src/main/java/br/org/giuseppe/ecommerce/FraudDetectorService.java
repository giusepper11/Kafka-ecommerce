package br.org.giuseppe.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class FraudDetectorService {
    public static void main(String[] args) throws InterruptedException {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER", fraudService::parseFraud, Order.class, new HashMap<String, String>())) {
            service.run();
        }
    }

    private void parseFraud(ConsumerRecord<String, Order> record) throws InterruptedException {
        System.out.println("------------------------------------------");
        System.out.println("Processando nova ordem, checando fraude!!!");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(1000);
        System.out.println("Ordem processada");

    }

}
