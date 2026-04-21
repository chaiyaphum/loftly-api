"""Unit tests for `selector.chat_classifier`.

Covers all three buckets (`explain`, `what-if`, `other`) + the extractor edge
cases (negative amounts, multi-category, mixed Thai-English, k-suffix, etc.).
No DB / no async — pure functions only.
"""

from __future__ import annotations

import pytest

from loftly.selector.chat_classifier import classify, extract_whatif_params

# ---------------------------------------------------------------------------
# classify()
# ---------------------------------------------------------------------------


def test_classify_thai_why_is_explain() -> None:
    assert classify("ทำไม KBank WISDOM อันดับ 1?") == "explain"


def test_classify_english_why_is_explain() -> None:
    assert classify("why is KBank WISDOM ranked first?") == "explain"


def test_classify_compare_keyword_is_explain() -> None:
    assert classify("เปรียบเทียบ card A vs card B") == "explain"


def test_classify_thai_whatif_amount_plus_category() -> None:
    # "ถ้าเพิ่ม dining อีก 20,000 ผลเปลี่ยนไหม?" — the spec's canonical example.
    assert classify("ถ้าเพิ่ม dining อีก 20,000 ผลเปลี่ยนไหม?") == "what-if"


def test_classify_english_whatif() -> None:
    assert classify("what if I increase dining by THB 20000?") == "what-if"


def test_classify_whatif_k_suffix() -> None:
    # "80k" must be recognised as a THB amount.
    assert classify("เพิ่ม travel 30k") == "what-if"


def test_classify_whatif_overrides_explain_when_both_signals_present() -> None:
    # Amount + category wins over "ทำไม" — re-rank is the more useful answer.
    assert classify("ทำไมถ้าเพิ่ม dining 20000 อันดับเปลี่ยน?") == "what-if"


def test_classify_greeting_is_other() -> None:
    assert classify("สวัสดีครับ") == "other"


def test_classify_empty_is_other() -> None:
    assert classify("") == "other"
    assert classify("   ") == "other"


def test_classify_amount_without_category_is_other() -> None:
    # Just a number, no category keyword — neither explain nor what-if.
    assert classify("20000") == "other"


def test_classify_category_without_amount_is_other() -> None:
    # Talks about dining, but no THB delta → not a what-if.
    assert classify("ชอบกินข้าวนอกบ้าน") == "other"


def test_classify_bare_small_number_does_not_trigger_whatif() -> None:
    # "2" alone isn't a THB amount; avoids false-positive on short digits.
    assert classify("อันดับ 2 คืออะไร") == "explain"  # "อันดับ" is explain trigger


# ---------------------------------------------------------------------------
# extract_whatif_params()
# ---------------------------------------------------------------------------


def test_extract_positive_delta() -> None:
    assert extract_whatif_params("เพิ่ม dining 20000") == {
        "category": "dining",
        "amount_thb_delta": 20_000,
    }


def test_extract_positive_delta_with_commas() -> None:
    assert extract_whatif_params("เพิ่ม dining 20,000") == {
        "category": "dining",
        "amount_thb_delta": 20_000,
    }


def test_extract_positive_delta_k_suffix() -> None:
    assert extract_whatif_params("เพิ่ม travel 30k") == {
        "category": "travel",
        "amount_thb_delta": 30_000,
    }


def test_extract_thai_multiplier_หมื่น() -> None:  # noqa: N802  # Thai suffix is intentional
    assert extract_whatif_params("เพิ่ม grocery 2หมื่น") == {
        "category": "grocery",
        "amount_thb_delta": 20_000,
    }


def test_extract_negative_delta() -> None:
    assert extract_whatif_params("ลด dining 10000") == {
        "category": "dining",
        "amount_thb_delta": -10_000,
    }


def test_extract_english_mixed() -> None:
    assert extract_whatif_params("what if grocery increases by 15,000 THB?") == {
        "category": "grocery",
        "amount_thb_delta": 15_000,
    }


def test_extract_multi_category_returns_none() -> None:
    # Two categories → ambiguous, classifier still says what-if but extractor
    # declines rather than guess.
    result = extract_whatif_params("เพิ่ม dining กับ travel 20000")
    assert result is None


def test_extract_multi_amount_returns_none() -> None:
    # Two distinct amounts → ambiguous delta.
    result = extract_whatif_params("ลด dining 10000 เพิ่ม 30000")
    assert result is None


def test_extract_on_non_whatif_returns_none() -> None:
    # An "explain" question has no delta to extract.
    assert extract_whatif_params("ทำไมอันดับ 1?") is None


def test_extract_with_thb_prefix() -> None:
    assert extract_whatif_params("dining เพิ่ม THB 25,000") == {
        "category": "dining",
        "amount_thb_delta": 25_000,
    }


def test_extract_with_baht_suffix() -> None:
    assert extract_whatif_params("เพิ่ม petrol 5000 บาท") == {
        "category": "petrol",
        "amount_thb_delta": 5_000,
    }


@pytest.mark.parametrize(
    "q,expected_cat",
    [
        ("เพิ่ม online 20000", "online"),
        ("เพิ่ม shopping online 15000", "online"),
        ("น้ำมัน 8000 เพิ่ม", "petrol"),
        ("เพิ่ม grocery ที่ Lotus 5000", "grocery"),
        ("เพิ่ม travel โรงแรม 50000", "travel"),
    ],
)
def test_extract_recognises_all_categories(q: str, expected_cat: str) -> None:
    result = extract_whatif_params(q)
    assert result is not None
    assert result["category"] == expected_cat


def test_extract_default_sign_is_positive_when_no_direction_word() -> None:
    # No explicit "เพิ่ม"/"ลด" → default positive.
    result = extract_whatif_params("dining 20000")
    assert result == {"category": "dining", "amount_thb_delta": 20_000}
