package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    // --- 修改点 1: 注入 UserRepository 以便查询数据库 ---
    @Autowired
    private UserRepository userRepository;

    @Test
    void task_four_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // 等待 Kafka 处理完所有消息
        Thread.sleep(2000);

        // --- 修改点 2: 自动查找 Wilbur 的余额并打印 ---
        System.out.println("\n\n\n========================================================");
        System.out.println("正在查找 Wilbur 的余额...");

        boolean found = false;
        Iterable<UserRecord> users = userRepository.findAll();
        for (UserRecord user : users) {
            if ("wilbur".equalsIgnoreCase(user.getName())) {
                System.out.println("find");
                System.out.println("Wilbur balance: " + user.getBalance());
                found = true;
                break;
            }
        }

        if (!found) {
            System.out.println("can't find the balance of the wilbur");
        }
        System.out.println("========================================================\n\n\n");

    }
}