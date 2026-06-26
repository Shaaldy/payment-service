package by.shaaldy.paymentservice.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.*;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "payments")
public class Payment {
  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  @Column(name = "order_id", updatable = false, nullable = false)
  private UUID orderId;

  @Column(name = "amount", nullable = false)
  private BigDecimal amount;

  @Column(name = "status", nullable = false)
  @Enumerated(EnumType.STRING)
  private PaymentStatus status;

  @CreationTimestamp
  @Column(name = "created_at", updatable = false)
  private Instant createdAt;
}
