package by.shaaldy.paymentservice.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import by.shaaldy.paymentservice.domain.OutboxMessage;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {
  List<OutboxMessage> findAllByOrderByCreatedAtAsc();
}
