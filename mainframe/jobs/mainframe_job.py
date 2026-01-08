import os
from datetime import datetime

BASE_DIR = r"D:\mainframe_to_analytics_dev\mainframe\data"

POS_DIR = os.path.join(BASE_DIR, "pos")
DATALAKE_DIR = os.path.join(BASE_DIR, "datalake")
BANK_DIR = os.path.join(BASE_DIR, "bank")
CUSTOMER_DIR = os.path.join(BASE_DIR, "customer")

BANK_CHARGE_RATE = 0.025
CUSTOMER_CHARGE_RATE = 0.015


class MainframeSettlementJob:

    def __init__(self):
        self.run_date = datetime.now().strftime("%Y%m%d")

        os.makedirs(DATALAKE_DIR, exist_ok=True)
        os.makedirs(BANK_DIR, exist_ok=True)
        os.makedirs(CUSTOMER_DIR, exist_ok=True)

    @staticmethod
    def to_implied_decimal(amount, scale=2, width=9):
        """
        Convert a float amount to integer representation without decimal,
        respecting mainframe implied decimal (V) convention.
        width = total digits including decimals
        scale = number of decimal digits
        """
        return f"{int(round(amount * (10 ** scale))):0{width}d}"

    def process(self):
        pos_file = os.path.join(POS_DIR, f"POS_TXN_{self.run_date}.DAT")

        datalake_file = os.path.join(
            DATALAKE_DIR, f"RAW_POS_DATALAKE_{self.run_date}.TXT"
        )

        customer_file = os.path.join(
            CUSTOMER_DIR, f"CUSTOMER_CHARGES_{self.run_date}.TXT"
        )

        bank_files = {}

        with open(pos_file, "r") as infile, \
             open(datalake_file, "w") as dl_out, \
             open(customer_file, "w") as cust_out:

            for line in infile:
                # --- parse fixed fields ---
                txn_date = line[0:10].strip()
                txn_time = line[11:19].strip()
                store_id = line[20:26].strip()
                terminal_id = line[27:31].strip()
                txn_id = line[32:44].strip()
                cust_id = line[45:55].strip()
                payment_mode = line[56:66].strip()
                partner_bank = line[67:82].strip()
                amount_paid = float(line[83:92].strip())
                currency = line[111:114].strip()
                txn_status = line[115:125].strip()

                # --- charges ---
                bank_charge_amt = amount_paid * BANK_CHARGE_RATE
                customer_charge_amt = amount_paid * CUSTOMER_CHARGE_RATE

                bank_payable = amount_paid + bank_charge_amt
                customer_payable = amount_paid + customer_charge_amt

                # --- convert to mainframe implied decimals ---
                amount_paid_str = self.to_implied_decimal(amount_paid)
                bank_payable_str = self.to_implied_decimal(bank_payable)
                customer_payable_str = self.to_implied_decimal(customer_payable)
                bank_charge_amt_str = self.to_implied_decimal(bank_charge_amt)
                customer_charge_amt_str = self.to_implied_decimal(customer_charge_amt)

                # --- RAW DATALAKE ---
                dl_out.write(
                    f"{txn_date:<10} {txn_time:<8} {store_id:<6} {terminal_id:<4} "
                    f"{txn_id:<12} {cust_id:<10} {payment_mode:<10} {partner_bank:<15} "
                    f"{amount_paid_str} {bank_payable_str} {customer_payable_str} "
                    f"{currency:<3} {txn_status:<10}\n"
                )

                # --- CUSTOMER FILE ---
                cust_out.write(
                    f"{txn_id:<12} {cust_id:<10} {amount_paid_str} "
                    f"{customer_charge_amt_str} {customer_payable_str}\n"
                )

                # --- BANK FILES ---
                if payment_mode != "CASH" and partner_bank != "NA":
                    if partner_bank not in bank_files:
                        bank_path = os.path.join(
                            BANK_DIR, f"BANK_{partner_bank}_{self.run_date}.DAT"
                        )
                        bank_files[partner_bank] = open(bank_path, "w")

                    bank_files[partner_bank].write(
                        f"{txn_id:<12} {store_id:<6} "
                        f"{amount_paid_str} {bank_charge_amt_str} "
                        f"{bank_payable_str}\n"
                    )

        for f in bank_files.values():
            f.close()


if __name__ == "__main__":
    job = MainframeSettlementJob()
    job.process()
    print("Mainframe settlement job completed.")
