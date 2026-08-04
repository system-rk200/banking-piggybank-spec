# 03. Модели процессов

В данном разделе представлены модели ключевых бизнес-процессов и сценарии взаимодействия микросервиса «Копилка» со смежными системами Банка.

---

## 1. Процесс подключения сервиса «Копилка» (BPMN)

Данный процесс описывает пользовательский сценарий создания Копилки в Мобильном приложении, включая пошаговую валидацию, выбор параметров и открытие счета в банковском ядре (Core Banking / АБС).

### Диаграмма бизнес-процесса BPMN

![Подключение сервиса «Копилка»](diagrams/connect_piggybank_bpmn.bpmn)

---

## 2. Интеграционные сценарии (Sequence Diagrams)

Для удобства проектирования и разработки взаимодействие микросервисов разбито на 3 ключевых изолированных сценария.

---

### 2.1. Сценарий A: Автопополнение сдачи при покупке (Event-Driven / Kafka)

**Описание:**  
Асинхронный процесс обработки транзакций. Микросервис вычитывает события транзакций из Kafka, рассчитывает сумму округления (Delta), проверяет остаток на карте клиента в АБС и выполняет автоматический перевод средств в Копилку.

```mermaid
sequenceDiagram
    autonumber
    participant Kafka as Kafka (payment-events)
    participant PB as PiggyBank Service
    participant ABS as Core Banking / ABS
    participant Notif as Notification Service

    activate Kafka
    Kafka->>PB: Потребление события (POS_PAYMENT / ECOM_PAYMENT)
    activate PB

    PB->>PB: 1. Валидация типа и расчет Delta (FR-CALC-01, FR-CALC-02)

    alt Баланс карты >= Delta (Средств достаточно)
        PB->>ABS: 2. Перевод Delta [idempotency_key] (FR-EXEC-01)
        activate ABS
        ABS-->>PB: 200 OK (Проводка выполнена)
        deactivate ABS

        PB->>Notif: 3. Отправка события для Push (FR-INFO-02.1)
        activate Notif
        Notif-->>PB: Push отправлен
        deactivate Notif
    else Баланс карты < Delta (Недостаточно средств)
        PB->>PB: Отмена списания: SKIPPED_INSUFFICIENT_FUNDS (FR-EXEC-02.3)
    end

    deactivate PB
    deactivate Kafka
```

---

### 2.2. Сценарий B: Подключение сервиса «Копилка» (REST API)

**Описание:**  
Синхронный REST-запрос от Мобильного приложения при завершении мастера подключения. Сервис проверяет бизнес-правило (одна Копилка на клиента) и инициирует открытие накопительного счета в банковском ядре (АБС).

```mermaid
sequenceDiagram
    autonumber
    actor App as Мобильное Приложение
    participant Gateway as API Gateway
    participant PB as PiggyBank Service
    participant ABS as Core Banking / ABS

    App->>Gateway: POST /v1/piggy-bank {step_size, card_ids} (FR-ACC-01.1)
    activate App
    activate Gateway
    Gateway->>PB: POST /api/v1/piggy-bank (FR-ACC-01.2)
    activate PB

    PB->>PB: Проверка: нет ли уже активной Копилки (FR-ACC-01.4)

    alt Активная Копилка уже существует
        PB-->>Gateway: 409 Conflict (PIGGY_BANK_ALREADY_EXISTS)
        Gateway-->>App: 409 Conflict ("Копилка уже подключена")
    else Копилки нет
        PB->>ABS: Запрос на открытие накопительного счета (FR-ACC-01.3)
        activate ABS
        ABS-->>PB: 201 Created {account_number: "40817..."}
        deactivate ABS

        PB->>PB: Сохранение настроек и привязка карт (FR-ACC-01.6)
        PB-->>Gateway: 201 Created {piggy_bank_id, status: "ACTIVE"}
        Gateway-->>App: 201 Created (Экран успешного подключения)
    end

    deactivate PB
    deactivate Gateway
    deactivate App
```

---

### 2.3. Сценарий C: Управление настройками и просмотр аналитики

**Описание:**  
Пользовательские сценарии управления уже созданной Копилкой: изменение шага округления (настройки вступают в силу для новых покупок) и получение истории накоплений с аналитикой за период.

```mermaid
sequenceDiagram
    autonumber
    actor App as Мобильное Приложение
    participant Gateway as API Gateway
    participant PB as PiggyBank Service

    %% Часть 1: Изменение шага
    rect rgb(245, 245, 255)
    note right of App: Изменение шага округления
    App->>Gateway: PATCH /v1/piggy-bank/settings {step_size: 500} (FR-ACC-02.1)
    activate App
    activate Gateway
    Gateway->>PB: Update Step Size
    activate PB
    PB-->>Gateway: 200 OK (Настройки обновлены)
    deactivate PB
    Gateway-->>App: 200 OK (Шаг изменен)
    deactivate Gateway
    deactivate App
    end

    %% Часть 2: Просмотр аналитики
    rect rgb(245, 255, 245)
    note right of App: Просмотр истории и аналитики
    App->>Gateway: GET /v1/piggy-bank/history?period=month (FR-INFO-01.1)
    activate App
    activate Gateway
    Gateway->>PB: Get History & Analytics (FR-INFO-01.2)
    activate PB
    PB-->>Gateway: 200 OK {total_accumulated, transactions}
    deactivate PB
    Gateway-->>App: 200 OK (Отображение виджета аналитики)
    deactivate Gateway
    deactivate App
    end
```