package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public DatabaseConduit(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository) {

        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }


    // Used by UserPopulator to save users
    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }


    // Used by Kafka Listener to process transactions
    public void process(Transaction transaction) {

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Invalid sender or recipient
        if (sender == null || recipient == null) {
            return;
        }

        // Sender does not have enough balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }


        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());


        // Save updated users
        userRepository.save(sender);
        userRepository.save(recipient);


        // Print balances to find waldorf's final balance
        System.out.println("Sender: " + sender.getName() +
                " Balance: " + sender.getBalance());

        System.out.println("Recipient: " + recipient.getName() +
                " Balance: " + recipient.getBalance());


        // Save transaction record
        TransactionRecord transactionRecord =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount()
                );

        transactionRecordRepository.save(transactionRecord);
    }
}