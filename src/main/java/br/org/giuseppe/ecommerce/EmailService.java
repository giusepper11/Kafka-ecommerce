package br.org.giuseppe.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static void main(String[] args) throws InterruptedException {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL", emailService::parseEmail)) {
            service.run();
        }
    }

    private void parseEmail(ConsumerRecord<String, String> record) throws InterruptedException {

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