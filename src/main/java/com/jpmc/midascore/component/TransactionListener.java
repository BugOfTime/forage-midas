package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive; // 导入新建的 Incentive 类
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate; // 导入 RestTemplate

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate = new RestTemplate();

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void onTransaction(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {


            String url = "http://localhost:8080/incentive";

            Incentive incentiveObj = restTemplate.postForObject(url, transaction, Incentive.class);
            float incentiveAmount = incentiveObj.getAmount();


            float amount = transaction.getAmount();


            sender.setBalance(sender.getBalance() - amount);


            recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

            userRepository.save(sender);
            userRepository.save(recipient);


            TransactionRecord record = new TransactionRecord(sender, recipient, amount);
            record.setIncentive(incentiveAmount);
            transactionRecordRepository.save(record);

            if (sender.getName().equals("wilbur")) {
                System.out.println("Answer Helper - Wilbur balance: " + sender.getBalance());
            }
            if (recipient.getName().equals("wilbur")) {
                System.out.println("Answer Helper - Wilbur balance: " + recipient.getBalance());
            }

        } else {
            System.out.println("Transaction discarded (Invalid): " + transaction);
        }
    }
}