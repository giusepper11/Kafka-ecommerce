package br.org.giuseppe.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class EmailService {
    public static void main(String[] args) throws InterruptedException {
        var emailService = new EmailService();
        try (var service = new KafkaService<Email>(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL", emailService::parseEmail, Email.class, new HashMap<String, String>())) {
            service.run();
        }
    }

    private void parseEmail(ConsumerRecord<String, Email> record) throws InterruptedException {
        System.out.println("------------------------------------------");
        System.out.println("Processando novo email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(1000);
        System.out.println("Email Enviado");
    }


}
