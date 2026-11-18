"""Generate a synthetic general ledger for a fictional Luxembourg company, with planted anomalies.

Outputs (CSV, all values as text, like a raw ERP export):
  journal_lines.csv    one row per journal line
  accounts.csv         chart of accounts
  users.csv            users with department and approval limit
  period_calendar.csv  monthly close dates
  answer_key.csv       entry_id, anomaly_type  -> the anomalies the audit tests must find
  dq_answer_key.csv    entry_id, issue         -> entries with data-quality defects (must be quarantined)

Usage:
  python data_generator/generate_ledger.py --out data/raw --scale 1.0 --seed 2025
"""
from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

YEAR = 2025
VAT_RATE = 0.17  # Luxembourg standard VAT rate

ACCOUNTS = [
    ("101000", "Cash - main bank account", "Asset"),
    ("101100", "Cash - payroll account", "Asset"),
    ("110000", "Trade receivables", "Asset"),
    ("120000", "Inventory", "Asset"),
    ("140000", "VAT receivable", "Asset"),
    ("150000", "Prepaid expenses", "Asset"),
    ("160000", "Fixed assets - equipment", "Asset"),
    ("169000", "Accumulated depreciation", "Asset"),
    ("200000", "Trade payables", "Liability"),
    ("210000", "Accrued expenses", "Liability"),
    ("220000", "VAT payable", "Liability"),
    ("230000", "Payroll liabilities", "Liability"),
    ("250000", "Bank loan", "Liability"),
    ("300000", "Share capital", "Equity"),
    ("310000", "Retained earnings", "Equity"),
    ("400000", "Sales - products", "Revenue"),
    ("410000", "Sales - services", "Revenue"),
    ("420000", "Other income", "Revenue"),
    ("500000", "Cost of goods sold", "Expense"),
    ("600000", "Salaries", "Expense"),
    ("601000", "Social charges", "Expense"),
    ("610000", "Rent", "Expense"),
    ("620000", "Utilities", "Expense"),
    ("630000", "IT services", "Expense"),
    ("640000", "Travel", "Expense"),
    ("650000", "Marketing", "Expense"),
    ("660000", "Professional fees", "Expense"),
    ("670000", "Depreciation expense", "Expense"),
    ("680000", "Bank charges", "Expense"),
    ("690000", "Interest expense", "Expense"),
]

# user_id, name, department, role, approval_limit (EUR)
USERS = [
    ("U001", "Lea Hoffmann", "Accounts Payable", "AP clerk", 25000),
    ("U002", "Tom Weber", "Accounts Payable", "AP clerk", 25000),
    ("U003", "Ana Silva", "Accounts Receivable", "AR clerk", 25000),
    ("U004", "Marc Schmit", "Accounts Receivable", "AR clerk", 25000),
    ("U005", "Sophie Klein", "Treasury", "Treasury officer", 100000),
    ("U006", "Hugo Muller", "Treasury", "Treasury officer", 100000),
    ("U007", "Nina Thill", "Payroll", "Payroll officer", 50000),
    ("U008", "Paul Reuter", "Finance", "GL accountant", 50000),
    ("U009", "Clara Meyer", "Finance", "GL accountant", 50000),
    ("U010", "Julie Dupont", "Finance", "Financial controller", 250000),
    ("BATCH01", "System batch", "System", "Automated posting", 0),
]
LIMIT = {u[0]: u[4] for u in USERS}
DEPT = {u[0]: u[2] for u in USERS}

AP_EXPENSE_ACCOUNTS = ["500000", "610000", "620000", "630000", "640000", "650000", "660000", "120000", "680000"]
MANUAL_DEBIT_ACCOUNTS = ["610000", "620000", "630000", "640000", "650000", "660000", "150000"]
VENDORS = ["Alpha Supplies", "Batiself", "Cactus Pro", "Datalux", "EnerLux", "Ferrero Office", "GreenCleaning",
           "HiTech Services", "Imprimerie Centrale", "JetTravel", "KPL Consulting", "LuxLogistics"]
CUSTOMERS = ["Arcelor Parts", "Benelux Retail", "Cargo Center", "Delta Bank", "Euro Fund Services",
             "Fiduciaire Nord", "Grand Hotel", "Horizon Pharma", "Infra Build", "Joyeux Foods"]

# Planted unusual department -> account pairs (each used at most twice, so they stay rare)
RARE_PAIRS = [
    ("U001", "310000", "200000"), ("U002", "400000", "200000"), ("U001", "250000", "200000"),
    ("U003", "600000", "110000"), ("U004", "690000", "110000"), ("U003", "160000", "110000"),
    ("U005", "650000", "101000"), ("U006", "420000", "101000"),
    ("U007", "400000", "230000"), ("U007", "630000", "230000"),
]


class Ledger:
    def __init__(self, seed: int, scale: float):
        self.rng = random.Random(seed)
        self.scale = scale
        self.lines: list[dict] = []
        self.answer_key: list[tuple[str, str]] = []
        self.dq_key: list[tuple[str, str]] = []
        self.next_id = 1
        self.business_days = [d for d in (date(YEAR, 1, 1) + timedelta(n) for n in range(365)) if d.weekday() < 5]
        self.close_dates = {m: self._close_date(m) for m in range(1, 13)}

    # ------------------------------------------------------------------ helpers
    def _close_date(self, month: int) -> date:
        """Period closes on the 5th business day of the following month."""
        d = date(YEAR + (month == 12), month % 12 + 1, 1)
        count = 0
        while True:
            if d.weekday() < 5:
                count += 1
                if count == 5:
                    return d
            d += timedelta(1)

    def new_id(self) -> str:
        eid = f"JE{self.next_id:07d}"
        self.next_id += 1
        return eid

    def cents(self, mu: float, sigma: float) -> int:
        """Log-normal amount in cents, never a multiple of 1,000 EUR (round amounts are planted on purpose)."""
        while True:
            c = int(round(self.rng.lognormvariate(mu, sigma) * 100))
            if c >= 100 and c % 100000 != 0:
                return c

    def work_time(self, d: date) -> datetime:
        return datetime(d.year, d.month, d.day, self.rng.randint(8, 17), self.rng.randint(0, 59), self.rng.randint(0, 59))

    def split(self, total: int, parts: int) -> list[int]:
        if parts == 1:
            return [total]
        cuts = sorted(self.rng.sample(range(1, total), parts - 1))
        return [b - a for a, b in zip([0] + cuts, cuts + [total])]

    def add_entry(self, posting: date, created: datetime, user: str, source: str, description: str,
                  debits: list[tuple[str, int]], credits: list[tuple[str, int]], reference: str = "",
                  entry_id: str | None = None) -> str:
        eid = entry_id or self.new_id()
        n = 1
        for acc, amt in debits:
            self.lines.append(self._line(eid, n, posting, created, acc, description, amt, 0, user, source, reference)); n += 1
        for acc, amt in credits:
            self.lines.append(self._line(eid, n, posting, created, acc, description, 0, amt, user, source, reference)); n += 1
        return eid

    @staticmethod
    def _line(eid, n, posting, created, acc, desc, debit_c, credit_c, user, source, ref):
        return {
            "entry_id": eid, "line_no": str(n), "posting_date": posting.isoformat(),
            "created_at": created.strftime("%Y-%m-%d %H:%M:%S"), "fiscal_period": f"{posting.year}-{posting.month:02d}",
            "account_id": acc, "description": desc,
            "debit": f"{debit_c / 100:.2f}" if debit_c else "", "credit": f"{credit_c / 100:.2f}" if credit_c else "",
            "currency": "EUR", "user_id": user, "source": source, "reference": ref,
        }

    def n(self, base: int) -> int:
        return max(1, int(round(base * self.scale)))

    # ------------------------------------------------------------------ normal activity
    def ap_invoice(self, d: date, created: datetime | None = None, entry_id: str | None = None,
                   ref: str | None = None, vendor: str | None = None, lines: list[tuple[str, int]] | None = None) -> str:
        vendor = vendor or self.rng.choice(VENDORS)
        ref = ref or f"INV-{self.rng.randint(10000, 99999)}"
        if lines is None:
            net = self.cents(7.0, 1.2)
            parts = self.split(net, self.rng.choice([1, 1, 2]))
            lines = [(self.rng.choice(AP_EXPENSE_ACCOUNTS), p) for p in parts]
        vat = int(round(sum(a for _, a in lines) * VAT_RATE))
        gross = sum(a for _, a in lines) + vat
        user = self.rng.choice(["U001", "U002"])
        return self.add_entry(d, created or self.work_time(d), user, "AP", f"Invoice {vendor} {ref}",
                              lines + [("140000", vat)], [("200000", gross)], ref, entry_id)

    def ar_invoice(self, d: date) -> str:
        net = self.cents(7.5, 1.1)
        vat = int(round(net * VAT_RATE))
        cust = self.rng.choice(CUSTOMERS)
        ref = f"SI-{self.rng.randint(100000, 999999)}"
        return self.add_entry(d, self.work_time(d), self.rng.choice(["U003", "U004"]), "AR", f"Sales invoice {cust} {ref}",
                              [("110000", net + vat)], [(self.rng.choice(["400000", "410000"]), net), ("220000", vat)], ref)

    def ap_payment(self, d: date) -> str:
        amt = self.cents(7.2, 1.2)
        return self.add_entry(d, self.work_time(d), self.rng.choice(["U005", "U006"]), "BANK",
                              f"Supplier payment {self.rng.choice(VENDORS)}", [("200000", amt)], [("101000", amt)])

    def ar_receipt(self, d: date) -> str:
        amt = self.cents(7.6, 1.1)
        return self.add_entry(d, self.work_time(d), self.rng.choice(["U005", "U006"]), "BANK",
                              f"Customer receipt {self.rng.choice(CUSTOMERS)}", [("101000", amt)], [("110000", amt)])

    def manual_amount(self, user: str) -> int:
        """Typical manual journal amount, kept away from the 95-100% approval band and never round."""
        lim = LIMIT[user] * 100
        while True:
            c = self.cents(8.0, 1.0)
            if not (0.95 * lim <= c < lim):
                return c

    def manual_journal(self, d: date, user: str, amount: int | None = None, created: datetime | None = None,
                       entry_id: str | None = None) -> str:
        amt = amount if amount is not None else self.manual_amount(user)
        debit_acc = self.rng.choice(MANUAL_DEBIT_ACCOUNTS)
        if self.rng.random() < 0.6:
            credit_acc, desc = "210000", "Month-end accrual"
        else:
            credit_acc = self.rng.choice([a for a in MANUAL_DEBIT_ACCOUNTS if a != debit_acc])
            desc = "Reclassification between cost accounts"
        return self.add_entry(d, created or self.work_time(d), user, "MANUAL", desc, [(debit_acc, amt)], [(credit_acc, amt)],
                              entry_id=entry_id)

    def month_end(self, month: int):
        last = max(d for d in self.business_days if d.month == month)
        dep = int(round(42000_00 + self.rng.randint(1, 99_99)))
        self.add_entry(last, datetime(last.year, last.month, last.day, 2, 15), "BATCH01", "SYSTEM",
                       "Monthly depreciation", [("670000", dep)], [("169000", dep)])
        sal = self.cents(12.1, 0.05)
        soc = int(round(sal * 0.15))
        self.add_entry(last, self.work_time(last), "U007", "PAYROLL", f"Payroll {YEAR}-{month:02d}",
                       [("600000", sal), ("601000", soc)], [("230000", sal + soc)])
        self.add_entry(last, self.work_time(last), "U007", "PAYROLL", f"Payroll payment {YEAR}-{month:02d}",
                       [("230000", sal + soc)], [("101100", sal + soc)])

    def normal_activity(self):
        for d in self.business_days:
            for _ in range(self.n(100)):
                self.ap_invoice(d)
            for _ in range(self.n(90)):
                self.ar_invoice(d)
            for _ in range(self.n(50)):
                self.ap_payment(d)
            for _ in range(self.n(50)):
                self.ar_receipt(d)
            for user in ("U008", "U009", "U010"):
                if self.rng.random() < 0.9:
                    self.manual_journal(d, user)
        for m in range(1, 13):
            self.month_end(m)

    # ------------------------------------------------------------------ planted anomalies
    def plant(self, kind: str, eid: str):
        self.answer_key.append((eid, kind))

    def plant_anomalies(self):
        r = self.rng
        finance = ["U008", "U009", "U010"]
        weekends = [date(YEAR, 1, 1) + timedelta(n) for n in range(365) if (date(YEAR, 1, 1) + timedelta(n)).weekday() >= 5]

        for _ in range(40):  # weekend postings
            d = r.choice(weekends)
            self.plant("weekend_posting", self.manual_journal(d, r.choice(finance), created=self.work_time(d)))

        for _ in range(35):  # late-night postings
            d = r.choice(self.business_days)
            hour = r.choice([22, 23, 0, 1, 2, 3, 4])
            created = datetime(d.year, d.month, d.day, hour, r.randint(0, 59), r.randint(0, 59))
            self.plant("late_night_posting", self.manual_journal(d, r.choice(finance), created=created))

        for _ in range(30):  # round amounts
            d = r.choice(self.business_days)
            user = r.choice(finance)
            amt = r.choice([10, 12, 15, 20, 25, 30, 40, 45]) * 1000_00
            self.plant("round_amount", self.manual_journal(d, user, amount=amt))

        for _ in range(25):  # just below the user's approval limit
            d = r.choice(self.business_days)
            user = r.choice(finance)
            lim = LIMIT[user] * 100
            amt = r.randint(int(0.95 * lim), lim - 1)
            if amt % 100000 == 0:
                amt += 37
            self.plant("below_approval_limit", self.manual_journal(d, user, amount=amt))

        for _ in range(25):  # posted after the period was closed (backdated)
            month = r.randint(1, 11)
            d = max(x for x in self.business_days if x.month == month)
            close = self.close_dates[month]
            later = [x for x in self.business_days if close < x <= close + timedelta(14)]
            self.plant("post_close_entry", self.manual_journal(d, r.choice(finance), created=self.work_time(r.choice(later))))

        for user, unusual, normal in RARE_PAIRS:  # unusual department-account combinations, twice each
            for _ in range(2):
                d = r.choice(self.business_days)
                amt = self.manual_amount(user)
                eid = self.add_entry(d, self.work_time(d), user, "MANUAL", "Adjustment", [(unusual, amt)], [(normal, amt)])
                self.plant("unusual_user_account", eid)

        originals = r.sample([l["entry_id"] for l in self.lines if l["source"] == "AP" and l["line_no"] == "1"], 30)
        by_entry: dict[str, list[dict]] = {}
        for l in self.lines:
            if l["entry_id"] in originals:
                by_entry.setdefault(l["entry_id"], []).append(l)
        for eid in originals:  # duplicate supplier invoices, keyed again a few minutes later
            ls = by_entry[eid]
            created = datetime.strptime(ls[0]["created_at"], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=r.randint(3, 50))
            new_id = self.new_id()
            for l in ls:
                c = dict(l); c["entry_id"] = new_id; c["created_at"] = created.strftime("%Y-%m-%d %H:%M:%S")
                self.lines.append(c)
            self.plant("duplicate_entry", new_id)

        for _ in range(20):  # unbalanced entries (one credit line altered)
            d = r.choice(self.business_days)
            eid = self.ar_invoice(d)
            line = next(l for l in reversed(self.lines) if l["entry_id"] == eid and l["credit"])
            line["credit"] = f"{float(line['credit']) + r.randint(1, 50000) / 100:.2f}"
            self.plant("unbalanced_entry", eid)

        for _ in range(8):  # high-risk combinations: round amount, keyed late on a weekend night
            d = r.choice(weekends)
            created = datetime(d.year, d.month, d.day, r.choice([22, 23]), r.randint(0, 59), r.randint(0, 59))
            eid = self.manual_journal(d, r.choice(["U008", "U009"]), amount=r.choice([20, 30, 35]) * 1000_00,
                                      created=created)
            for kind in ("weekend_posting", "late_night_posting", "round_amount"):
                self.plant(kind, eid)

        for _ in range(120):  # fabricated amounts by one user: first digits 7-9 (for the Benford analysis in R)
            d = r.choice(self.business_days)
            amt = r.randint(7000_00, 9999_99)
            if amt % 100000 == 0:
                amt += 11
            self.plant("benford_fabricated", self.manual_journal(d, "U009", amount=amt))

    # ------------------------------------------------------------------ data-quality defects
    def plant_dq_issues(self):
        r = self.rng

        def fresh(n):
            out = []
            for _ in range(n):
                out.append(self.ap_invoice(r.choice(self.business_days)))
            return out

        for eid in fresh(15):
            line = next(l for l in self.lines if l["entry_id"] == eid and l["line_no"] == "1")
            line["account_id"] = "999999"; self.dq_key.append((eid, "unknown_account"))
        for eid in fresh(10):
            for l in self.lines:
                if l["entry_id"] == eid: l["user_id"] = ""
            self.dq_key.append((eid, "missing_user"))
        for eid in fresh(8):
            line = next(l for l in self.lines if l["entry_id"] == eid and l["line_no"] == "1")
            line["credit"] = line["debit"]; self.dq_key.append((eid, "debit_and_credit_on_same_line"))
        for eid, bad in zip(fresh(6), ["2025-13-01", "31/02/2025", "2025-02-30", "N/A", "2025-00-10", "20250415"]):
            for l in self.lines:
                if l["entry_id"] == eid: l["posting_date"] = bad
            self.dq_key.append((eid, "invalid_posting_date"))
        for eid in fresh(5):
            line = next(l for l in self.lines if l["entry_id"] == eid and l["line_no"] == "1")
            line["debit"] = "-" + line["debit"]; self.dq_key.append((eid, "negative_amount"))

    # ------------------------------------------------------------------ output
    def write(self, out: str):
        os.makedirs(out, exist_ok=True)
        cols = list(self.lines[0].keys())
        self.rng.shuffle(self.lines)  # exports are rarely in a convenient order
        with open(os.path.join(out, "journal_lines.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(self.lines)
        with open(os.path.join(out, "accounts.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["account_id", "account_name", "account_type"]); w.writerows(ACCOUNTS)
        with open(os.path.join(out, "users.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["user_id", "name", "department", "role", "approval_limit"]); w.writerows(USERS)
        with open(os.path.join(out, "period_calendar.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["fiscal_period", "close_date"])
            for m, d in self.close_dates.items():
                w.writerow([f"{YEAR}-{m:02d}", d.isoformat()])
        with open(os.path.join(out, "answer_key.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["entry_id", "anomaly_type"]); w.writerows(sorted(self.answer_key))
        with open(os.path.join(out, "dq_answer_key.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["entry_id", "issue"]); w.writerows(sorted(self.dq_key))


def generate(out: str, scale: float = 1.0, seed: int = 2025) -> Ledger:
    led = Ledger(seed, scale)
    led.normal_activity()
    led.plant_anomalies()
    led.plant_dq_issues()
    led.write(out)
    return led


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw")
    p.add_argument("--scale", type=float, default=1.0, help="1.0 = about 200,000 journal lines")
    p.add_argument("--seed", type=int, default=2025)
    a = p.parse_args()
    led = generate(a.out, a.scale, a.seed)
    entries = len({l["entry_id"] for l in led.lines})
    print(f"{len(led.lines):,} lines, {entries:,} entries, {len(led.answer_key)} planted anomalies, "
          f"{len(led.dq_key)} entries with data-quality defects -> {a.out}")
