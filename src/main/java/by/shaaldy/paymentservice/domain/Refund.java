package by.shaaldy.paymentservice.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.*;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "refunds")
public class Refund {
  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  @Column(name = "payment_id", nullable = false, unique = true)
  private UUID paymentId;

  @Column(nullable = false)
  private BigDecimal amount;

  @Column(name = "created_at", nullable = false)
  @CreationTimestamp
  private Instant createdAt;
}
