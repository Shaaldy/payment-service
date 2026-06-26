package by.shaaldy.paymentservice.service;

import java.math.BigDecimal;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import by.shaaldy.paymentservice.domain.Payment;
import by.shaaldy.paymentservice.domain.PaymentStatus;
import by.shaaldy.paymentservice.messaging.event.OrderCreatedEvent;
import by.shaaldy.paymentservice.messaging.event.PaymentProcessedEvent;
import by.shaaldy.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {
  private final PaymentRepository paymentRepository;

  @Transactional
  public Optional<PaymentProcessedEvent> processPayment(OrderCreatedEvent event) {

    if (paymentRepository.existsByOrderId(event.orderId())) {
      return Optional.empty();
    }
    PaymentStatus outcome = decideOutcome(event.amount());
    Payment payment =
        Payment.builder().orderId(event.orderId()).status(outcome).amount(event.amount()).build();
    Payment saved = paymentRepository.save(payment);
    log.info(
        "Processing payment for order {}: amount={}, outcome={}",
        event.orderId(),
        event.amount(),
        outcome);
    return Optional.of(
        new PaymentProcessedEvent(
            saved.getId(), saved.getOrderId(), outcome == PaymentStatus.SUCCESS));
  }

  private PaymentStatus decideOutcome(BigDecimal amount) {
    boolean even = !amount.toBigInteger().testBit(0);
    return even ? PaymentStatus.SUCCESS : PaymentStatus.FAILED;
  }
}
