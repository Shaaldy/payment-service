package by.shaaldy.paymentservice.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import by.shaaldy.paymentservice.domain.Refund;

public interface RefundRepository extends JpaRepository<Refund, UUID> {
  boolean existsByPaymentId(UUID paymentId);
}
