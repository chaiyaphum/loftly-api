"""Versioned Thai-native system prompt for `typhoon_nlu_spend`.

Per `mvp/AI_PROMPTS.md §Prompt 3` — extract structured spend from free-text
Thai input. The target model is Typhoon (SambaNova-hosted, Thai-optimized), so
the prompt is written in Thai to keep the model in-distribution. Output is
STRICT JSON; no prose. The client validates against `schemas.spend_nlu.SpendProfile`.

Versioning convention (tracked here, not in a YAML file, because Phase 1
prompts live alongside code for simpler diffs):

- `PROMPT_NAME` + `PROMPT_VERSION` → slug stamped into PostHog events and
  Langfuse traces. Bump the version when you edit the prompt text.
- Exported constants (`SYSTEM_PROMPT_TH`, `USER_PROMPT_TEMPLATE_TH`,
  `EXPECTED_CATEGORIES`) are the only interface; importers never read this
  module for anything else.
"""

from __future__ import annotations

PROMPT_NAME = "typhoon_nlu_spend"
PROMPT_VERSION = "v1"

# Canonical category slugs the model must use. Mirrors `SpendCategory` in
# schemas/spend_nlu.py — keep in sync. Thai gloss is included in the prompt so
# the model doesn't have to guess which Thai word maps to which bucket.
EXPECTED_CATEGORIES: tuple[str, ...] = (
    "dining",
    "online",
    "grocery",
    "travel",
    "petrol",
    "default",
)

# Kept as a single Thai block for readability. The model sees this verbatim.
# If you edit this, bump PROMPT_VERSION above.
SYSTEM_PROMPT_TH = """คุณคือระบบ NLU ภาษาไทยของ Loftly สำหรับวิเคราะห์พฤติกรรมการใช้จ่ายบัตรเครดิตของผู้ใช้

หน้าที่ของคุณ: อ่านข้อความภาษาไทยที่ผู้ใช้พิมพ์แบบอิสระ แล้วสกัดข้อมูล 3 อย่าง:
1. ยอดใช้จ่ายต่อเดือน (บาท) — เป็นจำนวนเต็ม เช่น "80k" = 80000, "หนึ่งแสน" = 100000
2. สัดส่วนการใช้จ่ายแต่ละหมวด (เป็นเศษส่วนทศนิยม รวมกันเท่ากับ 1.0)
3. เป้าหมายของผู้ใช้ (miles / cashback / flexible)

หมวดที่ใช้ได้มี 6 หมวดเท่านั้น:
- "dining"  : กินข้าวนอกบ้าน, ร้านอาหาร, คาเฟ่, delivery (Foodpanda, GrabFood, LINE MAN)
- "online"  : ช้อปออนไลน์ (Shopee, Lazada, Amazon), subscription ดิจิทัล, in-app purchase
- "grocery" : ซูเปอร์มาร์เก็ต, ของใช้ในบ้าน, Lotus, Big C, Tops, Makro
- "travel"  : โรงแรม, ตั๋วเครื่องบิน, Grab/taxi, รถไฟฟ้า, เที่ยวต่างประเทศ
- "petrol"  : น้ำมันรถ, ปั๊มน้ำมัน (PTT, Shell, Esso, Caltex, Bangchak)
- "default" : อื่นๆ, ยอดที่เหลือที่ไม่เข้าหมวดไหน, ของเบ็ดเตล็ด

เป้าหมาย:
- "miles"    : ผู้ใช้อยากเก็บไมล์ เที่ยวต่างประเทศ ต้องการใช้ไมล์แลกตั๋ว
- "cashback" : ผู้ใช้อยากได้เงินคืนเข้าบัญชี ลดค่าใช้จ่าย
- "flexible" : ผู้ใช้ไม่ได้ระบุชัดเจน หรือต้องการคะแนนที่แลกได้หลายแบบ

กติกาเคร่งครัด:
- ตอบเป็น JSON ล้วน ห้ามมีคำอธิบาย ห้ามมี markdown ห้ามมี code fence
- หมวดที่ไม่ได้ถูกกล่าวถึงให้ใส่ 0.0 หรือไม่ต้องใส่ก็ได้ แต่ผลรวมต้องเท่ากับ 1.0
- ถ้าข้อความระบุจำนวนเงินแต่ละหมวด ให้คำนวณเศษส่วนจากยอดรวม
- ถ้าข้อความระบุแค่ว่า "ส่วนใหญ่" หรือ "เยอะสุด" ให้ประมาณสัดส่วนที่สมเหตุสมผล
- ยอดที่เหลือที่หาหมวดไม่ได้ให้ไปรวมในหมวด "default"
- ถ้าผู้ใช้ไม่ระบุยอดรวม ให้ประมาณจากยอดย่อยที่กล่าวถึง แต่ต้องไม่ต่ำกว่า 5000
- ใส่ "confidence" ระหว่าง 0.0 ถึง 1.0 สะท้อนความมั่นใจของคุณ

รูปแบบ JSON ที่ต้องตอบ:
{
  "monthly_spend_thb": <integer>,
  "spend_categories": {
    "dining":  <float 0-1>,
    "online":  <float 0-1>,
    "grocery": <float 0-1>,
    "travel":  <float 0-1>,
    "petrol":  <float 0-1>,
    "default": <float 0-1>
  },
  "goal": "miles" | "cashback" | "flexible",
  "confidence": <float 0-1>
}

ตัวอย่าง:

ผู้ใช้: "ผมใช้จ่ายเดือนละ 80k ส่วนใหญ่กินข้าวข้างนอก อยากเก็บไมล์"
ตอบ: {"monthly_spend_thb":80000,"spend_categories":{"dining":0.6,"online":0.1,"grocery":0.1,"travel":0.05,"petrol":0.05,"default":0.1},"goal":"miles","confidence":0.85}

ผู้ใช้: "เดือนละแสน ช้อปปิ้งออนไลน์ประมาณ 40000 ค่าน้ำมัน 10000 ที่เหลือใช้กินข้าว อยากได้เงินคืน"
ตอบ: {"monthly_spend_thb":100000,"spend_categories":{"dining":0.5,"online":0.4,"grocery":0.0,"travel":0.0,"petrol":0.1,"default":0.0},"goal":"cashback","confidence":0.9}
"""

# The user turn is just the raw text — keep it minimal so Typhoon focuses on
# the system prompt's rules. `.format(text_th=...)` substitutes the input.
USER_PROMPT_TEMPLATE_TH = "ข้อความจากผู้ใช้:\n{text_th}\n\nตอบ JSON:"


def prompt_slug() -> str:
    """PostHog/Langfuse identifier: `typhoon_nlu_spend@v1`."""
    return f"{PROMPT_NAME}@{PROMPT_VERSION}"


__all__ = [
    "EXPECTED_CATEGORIES",
    "PROMPT_NAME",
    "PROMPT_VERSION",
    "SYSTEM_PROMPT_TH",
    "USER_PROMPT_TEMPLATE_TH",
    "prompt_slug",
]
