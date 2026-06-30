package by.shaaldy.paymentservice.messaging;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import by.shaaldy.paymentservice.repository.PaymentRepository;
import by.shaaldy.paymentservice.repository.RefundRepository;

@SpringBootTest
public abstract class AbstractIntegrationTest {
  @Autowired protected RefundRepository refundRepository;
  @Autowired protected PaymentRepository paymentRepository;
  static final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

  static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

  static {
    kafka.start();
    postgres.start();
  }

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    registry.add("outbox.poll-interval", () -> 100);
  }

  @BeforeEach
  void cleanDatabase() {
    refundRepository.deleteAll();
    paymentRepository.deleteAll();
  }
}
