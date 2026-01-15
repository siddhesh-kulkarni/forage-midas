package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class KafkaTransactionListener {
    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core-group"
    )

    public void listen(Transaction transaction) {
        // Task 2 requirement:
        // Just receive the transaction, do nothing else for now
    }

}
