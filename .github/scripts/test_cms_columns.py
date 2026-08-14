"""The CMS core column list is written twice — assert the two copies never drift.

`download_cms_payments.py:CORE_COLUMNS` is the LAND-time guard: a program year whose CSV header
lacks any of these is refused rather than archived. `macros/cms_payment_columns.sql` is what the
three model trees generate their SELECT lists from. Neither can import the other — one is Python on
a runner, the other Jinja inside dbt — so this is the only thing holding them together.

Drift is silent in both directions and both are expensive:
  a column in the macro but not the guard  -> a year missing it is archived, and every engine's
                                              read of it fails mid-write, on paid capacity
  a column in the guard but not the macro  -> years are refused for a column nothing reads

Sibling of test_green_columns.py / test_nyc_columns.py / test_bts_columns.py, with two extra
assertions this dataset needs and they do not: that the SPARSE repeated groups survive, and that
CMS's own data-dictionary typo has not been copied in.
"""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

# The five product attributes CMS repeats five times over, and the two recipient attributes it
# repeats six times over. Their tail members are 83-100% NULL, which is the surface this dataset
# was added for.
PRODUCT_GROUPS = ("Covered_or_Noncovered_Indicator_",
                  "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_",
                  "Product_Category_or_Therapeutic_Area_",
                  "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_",
                  "Associated_Drug_or_Biological_NDC_",
                  "Associated_Device_or_Medical_Supply_PDI_")
RECIPIENT_GROUPS = ("Covered_Recipient_Primary_Type_", "Covered_Recipient_Specialty_")


def _python_list():
    src = open(os.path.join(ROOT, "download_cms_payments.py"), encoding="utf-8").read()
    body = re.search(r"^CORE_COLUMNS = \[(.*?)^\]", src, re.S | re.M)
    assert body, "CORE_COLUMNS not found in download_cms_payments.py"
    return re.findall(r'"([^"]+)"', body.group(1))


def _macro_list():
    src = open(os.path.join(ROOT, "macros", "cms_payment_columns.sql"), encoding="utf-8").read()
    body = re.search(r"macro cms_payment_columns\(\).*?return\(\[(.*?)\]\)", src, re.S)
    assert body, "cms_payment_columns() return list not found"
    return re.findall(r"'([^']+)'", body.group(1))


def test_the_two_column_lists_are_identical_and_in_the_same_order():
    # Order matters as well as membership: both lists are documented as "file order", and the
    # dialect SELECTs are generated positionally from the macro's.
    assert _macro_list() == _python_list()


def test_the_list_is_the_documented_ninety_one():
    cols = _macro_list()
    assert len(cols) == 91, f"expected all 91 source columns, got {len(cols)}"
    # Verified against the LITERAL header row of the published CSV, whose md5 is identical for
    # PY2019, 2021, 2023 and 2025 — so this count is the source's, not a subset anyone chose.
    assert cols[0] == "Change_Type" and cols[-1] == "Payment_Publication_Date"


def test_the_sparse_repeated_groups_are_all_present():
    """54 of the 91 columns are >50% NULL and that is why this dataset exists.

    Taking only the `_1` member of each group is the obvious tidy-up and would delete the entire
    sparsity surface — the tail members run 83%, 95%, 98% and 99% NULL, and the six-wide recipient
    groups are 100% NULL past `_1` in the sample. Nothing else in this project is sparse, so a
    quiet trim here costs the dataset its reason to be here."""
    cols = set(_macro_list())
    for prefix in PRODUCT_GROUPS:
        for i in range(1, 6):
            assert f"{prefix}{i}" in cols, \
                f"{prefix}{i} is part of the sparse surface this dataset exists to measure"
    for prefix in RECIPIENT_GROUPS:
        for i in range(1, 7):
            assert f"{prefix}{i}" in cols, \
                f"{prefix}{i} is part of the sparse surface this dataset exists to measure"


def test_the_skewed_categoricals_are_all_present():
    # Both regimes in one table, which is what makes this a new point rather than a wider bts.
    # nyc-shaped (86-100% single-value) and bts-shaped (hundreds of competing values), measured on
    # a 100 MB sample of PY2023.
    cols = set(_macro_list())
    for c in ("Nature_of_Payment_or_Transfer_of_Value",          # 92% one value
              "Form_of_Payment_or_Transfer_of_Value",            # 86%
              "Physician_Ownership_Indicator",                   # 87%
              "Related_Product_Indicator",                       # 95%
              "Dispute_Status_for_Publication",                  # 100%
              "Covered_Recipient_Specialty_1",                   # 302 values, 9.5% top
              "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",   # ~1,000, 10.5% top
              "Recipient_State"):                                # 56 values, 9.6% top
        assert c in cols, f"{c} is part of the skew this dataset exists to measure"


def test_the_data_dictionary_typo_has_not_been_copied_in():
    """CMS's own data dictionary spells field #78 `Covered_or_Nonccovered_Indicator_4`, double c.

    The published CSV spells it correctly, in every year. Rebuilding this list from the dictionary
    — the obvious thing to do when adding a program year — produces a column that matches nothing,
    and the land-time header guard would then refuse every year for a column that does not exist.
    Cheap to assert, and the failure it prevents costs a full re-drain to diagnose."""
    for cols in (_macro_list(), _python_list()):
        assert not [c for c in cols if "Nonccovered" in c], \
            "that spelling is the data dictionary's typo; the CSV header says 'Noncovered'"


def test_the_merge_key_and_the_dimension_key_are_both_present():
    # (file, Record_ID) is the incremental key on all four engines — this dataset is the one that
    # returns to a real keyed merge, because unlike TLC's trip records CMS publishes a unique id.
    # The payer id is the join key into dim_cms_payer and the reason for the whitespace assertion.
    cols = set(_macro_list())
    assert "Record_ID" in cols
    assert "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID" in cols
