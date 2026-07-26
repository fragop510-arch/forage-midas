package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final IncentiveClient incentiveClient;

    public DatabaseConduit(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository,
            IncentiveClient incentiveClient) {

        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.incentiveClient = incentiveClient;
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

        // Call Incentive API
        Incentive incentive = incentiveClient.getIncentive(transaction);

        float incentiveAmount = 0.0f;

        if (incentive != null) {
            incentiveAmount = incentive.getAmount();
        }

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount
        );

        // Save updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Save transaction record with incentive
        TransactionRecord transactionRecord =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentiveAmount
                );

        transactionRecordRepository.save(transactionRecord);
    }
}