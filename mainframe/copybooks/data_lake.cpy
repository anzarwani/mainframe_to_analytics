01  RAW-POS-RECORD.
    05 TXN-DATE            PIC X(10).
    05 FILLER              PIC X(1).

    05 TXN-TIME            PIC X(8).
    05 FILLER              PIC X(1).

    05 STORE-ID            PIC X(6).
    05 FILLER              PIC X(1).

    05 TERMINAL-ID         PIC X(4).
    05 FILLER              PIC X(1).

    05 TXN-ID              PIC X(12).
    05 FILLER              PIC X(1).

    05 CUST-ID             PIC X(10).
    05 FILLER              PIC X(1).

    05 PAYMENT-MODE        PIC X(10).
    05 FILLER              PIC X(1).

    05 PARTNER-BANK        PIC X(15).
    05 FILLER              PIC X(1).

    05 AMOUNT-PAID         PIC 9(7)V99.
    05 FILLER              PIC X(1).

    05 BANK-PAYABLE        PIC 9(7)V99.
    05 FILLER              PIC X(1).

    05 CUSTOMER-PAYABLE    PIC 9(7)V99.
    05 FILLER              PIC X(1).

    05 CURRENCY-CODE       PIC X(3).
    05 FILLER              PIC X(1).

    05 TXN-STATUS          PIC X(10).
