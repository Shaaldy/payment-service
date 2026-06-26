package by.shaaldy.paymentservice.repository;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import by.shaaldy.paymentservice.domain.Payment;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {
  boolean existsByOrderId(UUID orderId);

  Optional<Payment> findByOrderId(UUID orderId);
}
