package by.shaaldy.paymentservice.service;

import java.math.BigDecimal;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import by.shaaldy.paymentservice.domain.OutboxMessage;
import by.shaaldy.paymentservice.domain.Payment;
import by.shaaldy.paymentservice.domain.PaymentStatus;
import by.shaaldy.paymentservice.domain.Refund;
import by.shaaldy.paymentservice.messaging.event.payment.OrderCreatedEvent;
import by.shaaldy.paymentservice.messaging.event.payment.PaymentProcessedEvent;
import by.shaaldy.paymentservice.messaging.event.refund.OrderCancelledEvent;
import by.shaaldy.paymentservice.messaging.event.refund.RefundProcessedEvent;
import by.shaaldy.paymentservice.repository.OutboxRepository;
import by.shaaldy.paymentservice.repository.PaymentRepository;
import by.shaaldy.paymentservice.repository.RefundRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {
  private final PaymentRepository paymentRepository;
  private final RefundRepository refundRepository;
  private final OutboxRepository outboxRepository;
  private final ObjectMapper objectMapper;

  @Transactional
  public Optional<PaymentProcessedEvent> processPayment(OrderCreatedEvent event) {

    if (paymentRepository.existsByOrderId(event.orderId())) {
      return Optional.empty();
    }
    PaymentStatus outcome = decideOutcome(event.amount());
    Payment payment =
        Payment.builder().orderId(event.orderId()).status(outcome).amount(event.amount()).build();
    paymentRepository.save(payment);
    log.info(
        "Processing payment for order {}: amount={}, outcome={}",
        event.orderId(),
        event.amount(),
        outcome);
    PaymentProcessedEvent pay =
        new PaymentProcessedEvent(
            payment.getId(), payment.getOrderId(), payment.getStatus() == PaymentStatus.SUCCESS);
    String toPayLoad = objectMapper.writeValueAsString(pay);
    publishToOutbox(toPayLoad, "payment.processed");
    return Optional.of(pay);
  }

  @Transactional
  public Optional<RefundProcessedEvent> cancelPayment(OrderCancelledEvent event) {
    Optional<Payment> maybe = paymentRepository.findByOrderId(event.orderId());
    if (maybe.isEmpty()) {
      log.warn("order.cancelled for order {} but no payment found", event.orderId());
      return Optional.empty();
    }
    Payment payment = maybe.get();

    if (refundRepository.existsByPaymentId(payment.getId())) {
      return Optional.empty();
    }

    payment.setStatus(PaymentStatus.REFUNDED);
    Refund refund = Refund.builder().paymentId(payment.getId()).amount(payment.getAmount()).build();
    refundRepository.save(refund);
    log.info("Refunded payment {} amount {}", payment.getId(), payment.getAmount());

    RefundProcessedEvent ref = new RefundProcessedEvent(payment.getOrderId());
    publishToOutbox(objectMapper.writeValueAsString(ref), "payment.refunded");
    return Optional.of(ref);
  }

  private PaymentStatus decideOutcome(BigDecimal amount) {
    boolean even = !amount.toBigInteger().testBit(0);
    return even ? PaymentStatus.SUCCESS : PaymentStatus.FAILED;
  }

  private void publishToOutbox(String toPayLoad, String topic) {
    OutboxMessage message = OutboxMessage.builder().topic(topic).payload(toPayLoad).build();
    outboxRepository.save(message);
  }
}
