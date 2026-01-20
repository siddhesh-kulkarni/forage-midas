package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public KafkaTransactionListener(UserRepository userRepository, TransactionRepository transactionRepository){
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core-group"
    )

    public void listen(Transaction transaction) {
        // Task 2 requirement:
        // Just receive the transaction, do nothing else for now
        var senderOpt = userRepository.findById(transaction.getSenderId());
        var recipientOpt = userRepository.findById(transaction.getRecipientId());

        if(senderOpt.isEmpty() || recipientOpt.isEmpty())
        {
            return;
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        if(sender.getBalance() < transaction.getAmount()){
            return;
        }

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        Transaction record = new Transaction(sender, recipient, transaction.getAmount());
        transactionRepository.save(record);

    }

}
