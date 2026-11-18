from audit_lakehouse import dq


def entry(eid, debit="100.00", credit="100.00", **kw):
    """A balanced two-line entry; keyword arguments override fields on line 1."""
    base = {"entry_id": eid, "posting_date": "2025-03-14", "created_at": "2025-03-14 10:00:00",
            "fiscal_period": "2025-03", "user_id": "U008"}
    l1 = {**base, "line_no": "1", "account_id": "610000", "debit": debit, "credit": ""}
    l1.update(kw)
    l2 = {**base, "line_no": "2", "account_id": "210000", "debit": "", "credit": credit}
    return [l1, l2]


def run(make_lines, accounts, users, rows):
    checked = dq.apply_rules(make_lines(rows), accounts, users)
    valid, quarantine = dq.split_valid_and_quarantine(checked)
    return checked, valid, quarantine


def test_clean_entry_passes(make_lines, accounts, users):
    _, valid, quarantine = run(make_lines, accounts, users, entry("E1"))
    assert valid.count() == 2 and quarantine.count() == 0


def test_unknown_account_quarantines_whole_entry(make_lines, accounts, users):
    _, valid, quarantine = run(make_lines, accounts, users, entry("E1", account_id="999999") + entry("E2"))
    assert {r.entry_id for r in quarantine.collect()} == {"E1"}
    assert quarantine.count() == 2  # both lines of E1, not only the faulty one
    assert valid.count() == 2


def test_rule_codes_are_recorded(make_lines, accounts, users):
    _, _, quarantine = run(make_lines, accounts, users, entry("E1", user_id=""))
    rules = quarantine.first().entry_failed_rules
    assert "DQ02_USER_EXISTS" in rules


def test_invalid_dates_fail(make_lines, accounts, users):
    for bad in ["2025-13-01", "31/02/2025", "2025-02-30", "N/A"]:
        _, _, quarantine = run(make_lines, accounts, users, entry("E1", posting_date=bad))
        assert quarantine.count() == 2, bad


def test_posting_date_must_match_fiscal_period(make_lines, accounts, users):
    _, _, quarantine = run(make_lines, accounts, users, entry("E1", posting_date="2025-04-01"))
    assert "DQ03_VALID_POSTING_DATE" in quarantine.first().entry_failed_rules


def test_debit_and_credit_on_same_line_fails(make_lines, accounts, users):
    rows = entry("E1")
    rows[0]["credit"] = "100.00"  # line 1 now has both a debit and a credit
    _, _, quarantine = run(make_lines, accounts, users, rows)
    assert "DQ05_ONE_SIDED_AMOUNT" in quarantine.first().entry_failed_rules


def test_negative_and_non_numeric_amounts_fail(make_lines, accounts, users):
    for bad in ["-100.00", "abc"]:
        _, _, quarantine = run(make_lines, accounts, users, entry("E1", debit=bad))
        assert "DQ06_VALID_AMOUNT" in quarantine.first().entry_failed_rules, bad


def test_dq_results_count_failures(make_lines, accounts, users):
    checked, _, _ = run(make_lines, accounts, users, entry("E1", account_id="999999") + entry("E2"))
    res = {r.rule_code: r for r in dq.dq_results(checked, "t").collect()}
    assert res["DQ01_ACCOUNT_EXISTS"].failed_rows == 1
    assert res["DQ01_ACCOUNT_EXISTS"].checked_rows == 4
    assert res["DQ02_USER_EXISTS"].failed_rows == 0
