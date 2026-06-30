# payment-service

Сервис обработки платежей и возвратов. Реагирует на события заказов: проводит платёж и публикует результат, а на отмену оплаченного заказа фиксирует возврат в ledger. Идемпотентен на двух уровнях.

Часть проекта **food-order** (event-driven микросервисы на Spring Boot 4.1 / Java 21). Системная картина, архитектурная диаграмма и инструкция по запуску всего стека — в корневом репозитории: **[food-order-infra](https://github.com/Shaaldy/food-order-infra)**.

---

## Роль сервиса

- Слушает `order.created` → обрабатывает платёж → сохраняет `Payment` → публикует `PaymentProcessedEvent` (`payment.processed`).
- Слушает `order.cancelled` → создаёт запись о возврате в ledger → публикует `PaymentRefundedEvent` (`payment.refunded`).
- Своя изолированная база (общего хранилища с Order нет).

У сервиса **нет бизнес-REST** — он работает как Kafka-consumer. HTTP открыт только под Actuator (метрики/трейсинг).

---

## Внутреннее устройство

```
by.shaaldy.paymentservice
├── domain        Payment, PaymentStatus, Refund
├── repository    PaymentRepository (existsByOrderId),
│                 RefundRepository
├── service       PaymentService (обработка платежа + возврат через ledger)
└── messaging     PaymentEventListener (consume order.created / order.cancelled)
    │             PaymentEventPublisher (publish payment.processed / payment.refunded)
    └── event     OrderCreatedEvent, OrderCancelledEvent,
                  PaymentProcessedEvent, PaymentRefundedEvent
```

**Ключевые моменты этого сервиса:**

- **Двухуровневая идемпотентность.** Защита от at-least-once доставки Kafka: программный guard (`existsByOrderId` / проверка наличия возврата) **плюс** UNIQUE-констрейнт в БД. Программная проверка отсекает дубли дёшево; констрейнт — последний рубеж на гонках. Повторное событие не создаёт ни дубль платежа, ни дубль возврата.
- **Ledger-модель возвратов.** Возврат — это новая append-only запись `Refund` с самодостаточной суммой, **а не мутация исходного платежа**. `Refund` хранит `paymentId` обычным UUID (не `@OneToOne`) — запись самодостаточна, как в реальном финансовом учёте. Компенсация ≠ rollback.
- **Детерминированная заглушка шлюза.** Исход платежа: чётная целая часть суммы → `SUCCESS`, нечётная → `FAILED` (`amount.toBigInteger().testBit(0)`). В проде здесь был бы реальный платёжный провайдер.
- **Узкие контракты событий.** Публикует `boolean success`, а не свой enum `PaymentStatus`, чтобы Order не зависел от внутренних типов Payment.
- **String-десериализация.** Listener получает JSON-строку и сам делает `objectMapper.readValue(...)` на нужный тип под конкретный топик (Jackson 3) — это позволяет одному consumer'у обслуживать **несколько** типов событий (`order.created` и `order.cancelled`). Почему не `JsonDeserializer` — см. корневой README.
- **`spring-boot-starter-web` ради метрик.** Изначально headless-сервис; web-стартер добавлен, чтобы Actuator отдавал HTTP-эндпойнт для скрейпа Prometheus. Осознанный трейдофф (Tomcat в событийном сервисе) ради единообразия pull-модели метрик.

---

## Таблицы

- **`payments`** — `id`, `order_id` (UNIQUE → идемпотентность платежа), `amount`, `status`, `created_at`.
- **`refunds`** — append-only ledger возвратов; UNIQUE-ключ → идемпотентность возврата.

---

## Тестирование

- **Юниты** (`PaymentServiceTest`): детерминированный исход (чёт/нечёт → SUCCESS/FAILED), идемпотентность (skip при существующем `orderId`), кейсы усечения дробной части.
- **Интеграционные** (`*IT`, Testcontainers): реальные Kafka + PostgreSQL. Полный путь produce → listener → service → БД → publish → consume. Возврат проверяется отдельным `RefundSagaIT`. Async-проверки — через `Awaitility`; события читаются фильтром по бизнес-ключу (`orderId`), а не «ровно одна запись в топике».

```bash
./mvnw verify    # юниты (Surefire) + интеграционные (Failsafe)
```

CI: GitHub Actions гоняет `./mvnw verify` на каждый push в `main` и pull request.

---

## Стек

Java 21 · Spring Boot 4.1 · Spring for Apache Kafka · JPA/Hibernate · PostgreSQL · Flyway · Micrometer (Actuator/Prometheus, трейсинг через OpenTelemetry) · Maven · Lombok · Testcontainers · Awaitility · Spotless.

Запуск требует поднятой инфраструктуры (Kafka + PostgreSQL). Проще запускать весь стек через [food-order-infra](https://github.com/Shaaldy/food-order-infra).
