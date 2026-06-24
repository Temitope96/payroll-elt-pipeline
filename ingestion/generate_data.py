# Create a ficticious data for the project.

from faker import Faker
import pandas as pd
import random
import os
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

OUTPUT_DIR = os.environ.get("DATA_OUTPUT_DIR", "../data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

COUNTRIES = ["NG", "US", "GB", "CA", "DE", "FR", "IN", "BR", "ZA", "AU"]
CURRENCIES = {"NG":"NGN","US":"USD","GB":"GBP","CA":"CAD","DE":"EUR",
              "FR":"EUR","IN":"INR","BR":"BRL","ZA":"ZAR","AU":"AUD"}
DEPARTMENTS = ["Engineering","Finance","Operations","HR","Sales","Legal"]

def generate_employees(n=500):
    employees = []
    for i in range(1, n + 1):
        country = random.choice(COUNTRIES)
        employees.append({
            "employee_id": f"EMP{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "country_code": country,
            "department": random.choice(DEPARTMENTS),
            "hire_date": fake.date_between(start_date="-5y", end_date="today"),
            "salary_usd": round(random.uniform(20000, 150000), 2),
            "is_active": random.choices([True, False], weights=[90, 10])[0],
            "created_at": datetime.now().isoformat()
        })
    return pd.DataFrame(employees)

def generate_pay_periods(months=12):
    periods = []
    base = datetime(2024, 1, 1)
    for i in range(months):
        start = base + timedelta(days=30 * i)
        end = start + timedelta(days=29)
        periods.append({
            "period_id": f"PP{i+1:04d}",
            "period_start": start.date(),
            "period_end": end.date(),
            "pay_date": (end + timedelta(days=5)).date(),
            "period_year": start.year,
            "period_month": start.month
        })
    return pd.DataFrame(periods)

def generate_transactions(employees_df, periods_df, n=5000):
    transactions = []
    emp_ids = employees_df["employee_id"].tolist()
    period_ids = periods_df["period_id"].tolist()

    for i in range(1, n + 1):
        emp_id = random.choice(emp_ids)
        emp = employees_df[employees_df["employee_id"] == emp_id].iloc[0]
        country = emp["country_code"]
        gross = round(emp["salary_usd"] / 12, 2)
        deduction = round(gross * random.uniform(0.15, 0.30), 2)

        transactions.append({
            "transaction_id": f"TXN{i:07d}",
            "employee_id": emp_id,
            "period_id": random.choice(period_ids),
            "gross_pay_usd": gross,
            "deductions_usd": deduction,
            "net_pay_usd": round(gross - deduction, 2),
            "currency_code": CURRENCIES.get(country, "USD"),
            "status": random.choices(
                ["PROCESSED","PENDING","FAILED"],
                weights=[85, 10, 5])[0],
            "processed_at": fake.date_time_this_year().isoformat()
        })
    return pd.DataFrame(transactions)

if __name__ == "__main__":
    print("Generating employees...")
    emp_df = generate_employees(500)
    emp_df.to_csv(f"{OUTPUT_DIR}/employees.csv", index=False)

    print("Generating pay periods...")
    per_df = generate_pay_periods(12)
    per_df.to_csv(f"{OUTPUT_DIR}/pay_periods.csv", index=False)

    print("Generating transactions...")
    txn_df = generate_transactions(emp_df, per_df, 5000)
    txn_df.to_csv(f"{OUTPUT_DIR}/transactions.csv", index=False)

    print(f"Done! Files saved to {OUTPUT_DIR}/")
    print(f"  employees.csv: {len(emp_df)} rows")
    print(f"  pay_periods.csv: {len(per_df)} rows")
    print(f"  transactions.csv: {len(txn_df)} rows")