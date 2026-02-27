package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }


    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        // get sender & recipient from db
        UserRecord sender = databaseConduit.getUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.getUserById(transaction.getRecipientId());

        // validate transaction
        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());

            // save updated users to db
            databaseConduit.save(sender);
            databaseConduit.save(recipient);

            // save transaction record to db
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
            databaseConduit.save(transactionRecord);

        } else {
            System.out.println("Invalid transaction");
        }
    }
}
