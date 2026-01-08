import random 
import os
from datetime import datetime

POS_DATA = r"D:\mainframe_to_analytics_dev\mainframe\data\pos"

class POSDataGenerator:
    
    def __init__(self):
        self.store_ids = ["STR001", "STR002"]
        self.terminals = ["T01", "T02", "T03"]
        self.payment_modes = ["CASH", "CARD", "UPI"]
        self.banks = ["HDFC", "ICICI", "SBI", "NA"]
        self.currency = "INR"
        self.txn_counter = 1
        
    
    def generate_random_record(self):
        
        now = datetime.now()
        txn_date = now.strftime("%Y-%m-%d")
        txn_time = now.strftime("%H:%M:%S")
        
        store_id = random.choice(self.store_ids)
        terminal_id = random.choice(self.terminals)
        
        txn_id = f"TXN{self.txn_counter:06d}"
        self.txn_counter += 1
        
        cust_id = f"CUST{random.randint(10000,99999)}"
        
        payment_mode = random.choice(self.payment_modes)
        partner_bank = "NA" if payment_mode == "CASH" else random.choice(self.banks)

        amount_paid = round(random.uniform(50, 2000), 2)

        txn_status = random.choice(["SUCCESS", "FAILED"])

        return {
            "txn_date": txn_date,
            "txn_time": txn_time,
            "store_id": store_id,
            "terminal_id": terminal_id,
            "txn_id": txn_id,
            "cust_id": cust_id,
            "payment_mode": payment_mode,
            "partner_bank": partner_bank,
            "amount_paid": amount_paid,
            "currency": self.currency,
            "txn_status": txn_status
        }
        
    def generate_file(self, record_count=10):
        os.makedirs(POS_DATA, exist_ok=True)

        file_date = datetime.now().strftime("%Y%m%d")
        file_name = f"POS_TXN_{file_date}.DAT"
        file_path = os.path.join(POS_DATA, file_name)

        with open(file_path, "w") as f:
            for _ in range(record_count):
                rec = self.generate_random_record()

                line = (
                    f"{rec['txn_date']:<10} "        # TXN-DATE
                    f"{rec['txn_time']:<8} "         # TXN-TIME
                    f"{rec['store_id']:<6} "         # STORE-ID
                    f"{rec['terminal_id']:<4} "      # TERMINAL-ID
                    f"{rec['txn_id']:<12} "           # TXN-ID
                    f"{rec['cust_id']:<10} "          # CUST-ID
                    f"{rec['payment_mode']:<10} "     # PAYMENT-MODE
                    f"{rec['partner_bank']:<15} "     # PARTNER-BANK
                    f"{rec['amount_paid']:09.2f} "    # AMOUNT-BILLED
                    f"{rec['amount_paid']:09.2f} "    # AMOUNT-PAID
                    f"{rec['currency']:<3} "          # CURRENCY
                    f"{rec['txn_status']:<10}   \n"   # TXN-STATUS + trailing filler
                )

                f.write(line)

        return file_path
    
    
if __name__ == "__main__":
    gen = POSDataGenerator()
    output_file = gen.generate_file(record_count=50)
    print(f"POS file generated: {output_file}")

        
        
        
        
