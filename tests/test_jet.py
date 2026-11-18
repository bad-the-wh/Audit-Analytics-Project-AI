import itertools

from audit_lakehouse import dq, gold, jet

_amounts = itertools.count(1)


def manual(eid, amount=None, created="2025-03-14 10:00:00", posting="2025-03-14", user="U008",
           source="MANUAL", debit_acc="610000", credit_acc="210000", credit_amount=None, desc="Accrual"):
    # Each test entry gets a distinct amount by default, so unrelated entries never look like duplicates.
    amount = amount or f"{1000 + next(_amounts)}.37"
    base = {"entry_id": eid, "posting_date": posting, "created_at": created, "fiscal_period": posting[:7],
            "user_id": user, "source": source, "description": desc}
    return [
        {**base, "line_no": "1", "account_id": debit_acc, "debit": amount, "credit": ""},
        {**base, "line_no": "2", "account_id": credit_acc, "debit": "", "credit": credit_amount or amount},
    ]


def flags(make_lines, accounts, users, periods, rows):
    silver, _ = dq.split_valid_and_quarantine(dq.apply_rules(make_lines(rows), accounts, users))
    lines = gold.fact_journal_lines(silver, accounts, users)
    entries = gold.fact_journal_entries(lines, users)
    res = jet.run_journal_entry_tests(entries, lines, periods)
    return {r.entry_id: set(r.flags) for r in res.collect()}


# A background of normal Finance activity so that Finance -> 610000/210000 is a common pair.
BACKGROUND = [l for i in range(10) for l in manual(f"N{i}")]


def test_normal_entries_raise_no_flag(make_lines, accounts, users, periods):
    f = flags(make_lines, accounts, users, periods, BACKGROUND)
    assert all(v == set() for v in f.values())


def test_weekend_and_late_night(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("W", created="2025-03-15 11:00:00", posting="2025-03-15") \
        + manual("L", created="2025-03-14 23:30:00") + manual("E", created="2025-03-14 04:59:00")
    f = flags(make_lines, accounts, users, periods, rows)
    assert f["W"] == {"weekend_posting"}
    assert f["L"] == {"late_night_posting"}
    assert f["E"] == {"late_night_posting"}


def test_system_postings_are_exempt_from_time_rules(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("S", created="2025-03-15 02:00:00", posting="2025-03-15", user="BATCH01",
                               source="SYSTEM")
    f = flags(make_lines, accounts, users, periods, rows)
    assert "weekend_posting" not in f["S"] and "late_night_posting" not in f["S"]


def test_round_amount_threshold(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("R", amount="20000.00") + manual("SMALL", amount="9000.00") \
        + manual("CENTS", amount="20000.01")
    f = flags(make_lines, accounts, users, periods, rows)
    assert "round_amount" in f["R"]
    assert "round_amount" not in f["SMALL"] and "round_amount" not in f["CENTS"]


def test_below_approval_limit_band(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("IN", amount="49200.15") + manual("AT", amount="50000.01") \
        + manual("UNDER", amount="47499.99")
    f = flags(make_lines, accounts, users, periods, rows)
    assert "below_approval_limit" in f["IN"]
    assert "below_approval_limit" not in f["AT"] and "below_approval_limit" not in f["UNDER"]


def test_unbalanced(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("U", amount="100.00", credit_amount="100.50")
    assert "unbalanced_entry" in flags(make_lines, accounts, users, periods, rows)["U"]


def test_duplicate_flags_only_the_later_copy(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("D1", amount="777.77", desc="Invoice X 123") \
        + manual("D2", amount="777.77", desc="Invoice X 123", created="2025-03-14 10:20:00")
    f = flags(make_lines, accounts, users, periods, rows)
    assert "duplicate_entry" not in f["D1"]
    assert "duplicate_entry" in f["D2"]


def test_post_close(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("P", posting="2025-03-31", created="2025-04-09 10:00:00") \
        + manual("OK", posting="2025-03-31", created="2025-04-07 10:00:00")
    f = flags(make_lines, accounts, users, periods, rows)
    assert "post_close_entry" in f["P"]
    assert "post_close_entry" not in f["OK"]


def test_unusual_department_account(make_lines, accounts, users, periods):
    rows = BACKGROUND + manual("X", debit_acc="400000")
    f = flags(make_lines, accounts, users, periods, rows)
    assert "unusual_user_account" in f["X"]
    assert all("unusual_user_account" not in f[f"N{i}"] for i in range(10))
