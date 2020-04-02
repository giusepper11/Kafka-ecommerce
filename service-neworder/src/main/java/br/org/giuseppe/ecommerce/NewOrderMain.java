package br.org.giuseppe.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var orderDispatcher = new KafkaDispatcher<Order>();
             var emailDispatcher = new KafkaDispatcher<Email>()
        ) {

            for (var i = 0; i < 10; i++) {
                var userId = UUID.randomUUID().toString();
                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                var order = new Order(userId, orderId, amount);

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
                var email = new Email("Nova Compra", "Bem vindo! Estamos processando sua ordem!");
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
            }

        }

    }


}
