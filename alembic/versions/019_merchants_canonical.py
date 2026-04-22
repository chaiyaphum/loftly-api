"""019 — merchants_canonical + promos_merchant_canonical_map.

Adds canonical merchant entities for the Merchant Reverse Lookup surface
(`/merchants/[slug]`) per `mvp/SCHEMA.md §15 + §16` and the Q18 ratification
in `STRATEGY.md` Decision log (2026-04-22).

Why: proprietary answer for "which card is best at <merchant>?" queries,
mitigating Risk 1 (AI Overviews cratering organic search). The canonical
table de-duplicates messy upstream merchant strings from deal-harvester
(`Starbucks` / `สตาร์บัคส์` / `STARBUCKS COFFEE` → one canonical row) so
the `/merchants/[slug]` page can render stable ranked cards × promos × value.

Design notes:
- `alt_names` stored as Postgres `text[]` so a GIN index supports exact-name
  autocomplete. SQLite fallback is portable JSON (pg-only features skipped).
- Full-text search index combines display names + alt_names via
  `to_tsvector('simple', ...)` — Postgres-only. SQLite path relies on app-
  level trigram-ish fuzzy match (Levenshtein) until the tests upgrade.
- Soft-delete via `status='disabled'` — never hard-delete because map rows
  keep audit trail.
- Self-referential FK `merged_into_id` supports `/admin/merchants/{id}/merge`
  and `/split` (routes stubbed separately; this is pure DDL).

Seed:
- 50 curated top Thai brands (Starbucks, Grab variants, Shopee, Lazada, ...)
  with stable UUIDs `11ff1170-0000-4000-8000-00000000000N` (N = 1..50). UUIDs
  are hand-chosen so frontend/test fixtures and JSON-LD fixtures stay stable
  across deploys.

Revision ID: 019_merchants_canonical
Revises: 018_promos_active_idx
Create Date: 2026-04-22
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import (
    is_postgres,
    json_type,
    now_default,
    string_array_type,
    uuid_type,
)

revision: str = "019_merchants_canonical"
down_revision: str | None = "018_promos_active_idx"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Stable UUID prefix for seeded merchants. Digits 1..50 fill the last slot so
# any fixture / frontend / test that hardcodes a reference stays consistent.
_SEED_UUID_TEMPLATE = "11ff1170-0000-4000-8000-{:012d}"


# (slug, display_name_th, display_name_en, merchant_type, alt_names, category_default)
# Covers retail, F&B, ecommerce, travel, service — drives JSON-LD emission.
_SEEDED_MERCHANTS: list[tuple[str, str, str, str, list[str], str | None]] = [
    ("starbucks", "สตาร์บัคส์", "Starbucks", "fnb", ["Starbucks TH", "STARBUCKS COFFEE", "สตาร์บัคส์"], "dining-restaurants"),
    ("grab-food", "แกร็บ ฟู้ด", "Grab Food", "fnb", ["GrabFood", "Grab (Food)", "แกร็บฟู้ด"], "dining-restaurants"),
    ("grab-rides", "แกร็บ", "Grab", "service", ["GrabTaxi", "Grab (Rides)", "แกร็บไบค์"], None),
    ("shopee", "ช้อปปี้", "Shopee", "ecommerce", ["Shopee TH", "shopee.co.th", "ช้อปปี้"], "shopping"),
    ("lazada", "ลาซาด้า", "Lazada", "ecommerce", ["Lazada TH", "lazada.co.th", "ลาซาด้า"], "shopping"),
    ("seven-eleven", "เซเว่น อีเลฟเว่น", "7-Eleven", "retail", ["7-11", "เซเว่น", "7 Eleven", "CPALL"], "grocery"),
    ("central-department-store", "เซ็นทรัล ห้างสรรพสินค้า", "Central Department Store", "retail", ["Central", "Central Dept Store", "เซ็นทรัล"], "shopping"),
    ("central-restaurants-group", "เซ็นทรัล เรสเตอรองส์ กรุ๊ป", "Central Restaurants Group", "fnb", ["CRG", "Central Restaurants"], "dining-restaurants"),
    ("siam-paragon", "สยามพารากอน", "Siam Paragon", "retail", ["Paragon", "สยามพารากอน"], "shopping"),
    ("siam-discovery", "สยามดิสคัฟเวอรี่", "Siam Discovery", "retail", ["สยามดิสคัฟเวอรี่"], "shopping"),
    ("iconsiam", "ไอคอนสยาม", "ICONSIAM", "retail", ["Icon Siam", "ไอคอนสยาม"], "shopping"),
    ("foodpanda", "ฟู้ดแพนด้า", "Foodpanda", "fnb", ["Food Panda", "foodpanda TH", "ฟู้ดแพนด้า"], "dining-restaurants"),
    ("agoda", "อโกด้า", "Agoda", "travel", ["agoda.com", "อโกด้า"], "travel"),
    ("booking-com", "บุ๊กกิ้ง ดอทคอม", "Booking.com", "travel", ["Booking", "booking.com"], "travel"),
    ("expedia", "เอ็กซ์พีเดีย", "Expedia", "travel", ["Expedia TH"], "travel"),
    ("bts", "รถไฟฟ้า BTS", "BTS Skytrain", "service", ["BTS", "รถไฟฟ้าบีทีเอส", "บีทีเอส"], None),
    ("mrt", "รถไฟฟ้า MRT", "MRT", "service", ["MRT", "รถไฟฟ้าใต้ดิน", "รถไฟฟ้ามหานคร"], None),
    ("tops-supermarket", "ท็อปส์", "Tops Supermarket", "retail", ["Tops", "Tops Market", "ท็อปส์ ซูเปอร์มาร์เก็ต"], "grocery"),
    ("makro", "แม็คโคร", "Makro", "retail", ["Siam Makro", "แม็คโคร"], "grocery"),
    ("big-c", "บิ๊กซี", "Big C", "retail", ["BigC", "Big C Supercenter", "บิ๊กซี"], "grocery"),
    ("lotuss", "โลตัส", "Lotus's", "retail", ["Tesco Lotus", "Lotus", "โลตัส", "เทสโก้โลตัส"], "grocery"),
    ("cp-fresh-mart", "ซีพี เฟรชมาร์ท", "CP Fresh Mart", "retail", ["CP Freshmart", "ซีพี เฟรชมาร์ท"], "grocery"),
    ("villa-market", "วิลล่า มาร์เก็ต", "Villa Market", "retail", ["Villa", "วิลล่ามาร์เก็ต"], "grocery"),
    ("gourmet-market", "กูร์เมต์ มาร์เก็ต", "Gourmet Market", "retail", ["Gourmet", "กูร์เมต์"], "grocery"),
    ("terminal-21", "เทอร์มินอล 21", "Terminal 21", "retail", ["Terminal21", "เทอร์มินอล21"], "shopping"),
    ("mbk-center", "มาบุญครอง", "MBK Center", "retail", ["MBK", "มาบุญครอง", "เอ็มบีเค"], "shopping"),
    ("the-mall", "เดอะ มอลล์", "The Mall", "retail", ["The Mall Group", "เดอะมอลล์"], "shopping"),
    ("emporium", "ดิ เอ็มโพเรียม", "Emporium / EmQuartier", "retail", ["Emporium", "EmQuartier", "เอ็มโพเรียม", "เอ็มควอเทียร์"], "shopping"),
    ("robinson", "โรบินสัน", "Robinson", "retail", ["Robinsons", "โรบินสัน"], "shopping"),
    ("esso", "เอสโซ่", "Esso", "retail", ["Esso Thailand", "เอสโซ่"], "petrol"),
    ("ptt-station", "ปตท. สเตชั่น", "PTT Station", "retail", ["PTT", "ปตท", "ปั๊ม ปตท"], "petrol"),
    ("shell", "เชลล์", "Shell", "retail", ["Shell Thailand", "เชลล์"], "petrol"),
    ("bangchak", "บางจาก", "Bangchak", "retail", ["Bangchak Petroleum", "บางจาก"], "petrol"),
    ("true-coffee", "ทรู คอฟฟี่", "True Coffee", "fnb", ["TrueCoffee", "ทรูคอฟฟี่"], "dining-restaurants"),
    ("amazon-cafe", "คาเฟ่ อเมซอน", "Cafe Amazon", "fnb", ["Amazon Cafe", "Cafe Amazon", "คาเฟ่อเมซอน"], "dining-restaurants"),
    ("au-bon-pain", "โอ บอง แปง", "Au Bon Pain", "fnb", ["ABP", "โอบองแปง"], "dining-restaurants"),
    ("kfc", "เคเอฟซี", "KFC", "fnb", ["KFC Thailand", "เคเอฟซี"], "dining-restaurants"),
    ("mcdonalds", "แมคโดนัลด์", "McDonald's", "fnb", ["McDonald", "McD", "แมคโดนัลด์"], "dining-restaurants"),
    ("pizza-hut", "พิซซ่า ฮัท", "Pizza Hut", "fnb", ["Pizza Hut Thailand", "พิซซ่าฮัท"], "dining-restaurants"),
    ("the-pizza-company", "เดอะ พิซซ่า คอมปะนี", "The Pizza Company", "fnb", ["Pizza Company", "เดอะพิซซ่าคอมปะนี"], "dining-restaurants"),
    ("mk-suki", "เอ็มเค สุกี้", "MK Suki", "fnb", ["MK Restaurant", "MK", "เอ็มเค", "สุกี้เอ็มเค"], "dining-restaurants"),
    ("fuji-restaurant", "ฟูจิ", "Fuji Restaurant", "fnb", ["Fuji", "ฟูจิเรสเตอรองต์"], "dining-restaurants"),
    ("coca-suki", "โคคา สุกี้", "Coca Suki", "fnb", ["Coca", "โคคา"], "dining-restaurants"),
    ("bar-b-q-plaza", "บาร์บีคิว พลาซ่า", "Bar-B-Q Plaza", "fnb", ["BBQ Plaza", "บาร์บีคิวพลาซ่า", "บาร์บีกอน"], "dining-restaurants"),
    ("sukishi", "ซูกิชิ", "Sukishi", "fnb", ["Sukishi Korean Charcoal Grill", "ซูกิชิ"], "dining-restaurants"),
    ("oishi", "โออิชิ", "Oishi", "fnb", ["Oishi Group", "โออิชิ"], "dining-restaurants"),
    ("jim-thompson", "จิม ทอมป์สัน", "Jim Thompson", "retail", ["Jim Thompson Thai Silk", "จิมทอมป์สัน"], "shopping"),
    ("muji-thailand", "มูจิ", "Muji (Thailand)", "retail", ["MUJI", "มูจิ"], "shopping"),
    ("uniqlo-thailand", "ยูนิโคล่", "Uniqlo (Thailand)", "retail", ["UNIQLO", "ยูนิโคล่"], "shopping"),
    ("hm-thailand", "เอชแอนด์เอ็ม", "H&M (Thailand)", "retail", ["H&M", "HM", "เอชแอนด์เอ็ม"], "shopping"),
]


def _uuid_pk() -> sa.Column[object]:
    if is_postgres():
        return sa.Column(
            "id",
            uuid_type(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        )
    return sa.Column("id", uuid_type(), primary_key=True)


def upgrade() -> None:
    # --- merchants_canonical --------------------------------------------------
    op.create_table(
        "merchants_canonical",
        _uuid_pk(),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("display_name_th", sa.Text(), nullable=False),
        sa.Column("display_name_en", sa.Text(), nullable=False),
        sa.Column("category_default", sa.Text(), nullable=True),
        sa.Column(
            "alt_names",
            string_array_type(),
            nullable=False,
            server_default=(sa.text("'{}'::text[]") if is_postgres() else sa.text("'[]'")),
        ),
        sa.Column("logo_url", sa.Text(), nullable=True),
        sa.Column("description_th", sa.Text(), nullable=True),
        sa.Column("description_en", sa.Text(), nullable=True),
        sa.Column("merchant_type", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column(
            "merged_into_id",
            uuid_type(),
            sa.ForeignKey(
                "merchants_canonical.id",
                name="fk_merchants_canonical_merged_into",
                ondelete="SET NULL",
            ),
            nullable=True,
        ),
        sa.Column(
            "seo_meta",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.CheckConstraint(
            "merchant_type IN ('retail','fnb','ecommerce','travel','service')",
            name="merchants_canonical_merchant_type_check",
        ),
        sa.CheckConstraint(
            "status IN ('active','pending_review','merged','disabled')",
            name="merchants_canonical_status_check",
        ),
        sa.UniqueConstraint("slug", name="merchants_canonical_slug_key"),
    )

    # Indexes.
    op.create_index(
        "idx_merchants_canonical_slug",
        "merchants_canonical",
        ["slug"],
    )
    # Partial on active rows — hot-path filter for listing + search.
    if is_postgres():
        op.create_index(
            "idx_merchants_canonical_active",
            "merchants_canonical",
            ["status"],
            postgresql_where=sa.text("status = 'active'"),
        )
        # GIN on alt_names for exact autocomplete path.
        op.execute(
            "CREATE INDEX idx_merchants_canonical_altnames_gin "
            "ON merchants_canonical USING GIN (alt_names);"
        )
        # NOTE: The originally-planned full-text GIN index on the concatenation
        # of display_name_th + display_name_en + alt_names was dropped here
        # because Postgres requires IMMUTABLE functions in expression indexes
        # and `array_to_string` + `coalesce(...) || ...` is classified STABLE
        # (collation-dependent). Autocomplete today uses:
        #   1. the GIN on alt_names above for exact-match expansion, and
        #   2. trigram similarity via pg_trgm at query time in the search
        #      service (see src/loftly/services/merchant_search.py).
        # If full-text search becomes a hot path, a later migration should add
        # a STORED generated column (`search_vector tsvector GENERATED ALWAYS
        # AS (to_tsvector('simple', ...)) STORED`) and index that — which
        # keeps the IMMUTABLE constraint satisfied. Tracked as §9.1 follow-up.
        # Partial on merged rows for split rollback lookups.
        op.create_index(
            "idx_merchants_canonical_merged_into",
            "merchants_canonical",
            ["merged_into_id"],
            postgresql_where=sa.text("merged_into_id IS NOT NULL"),
        )
    else:
        # SQLite fallback: simple status index (no partial) — app filters anyway.
        op.create_index(
            "idx_merchants_canonical_status",
            "merchants_canonical",
            ["status"],
        )
        op.create_index(
            "idx_merchants_canonical_merged_into",
            "merchants_canonical",
            ["merged_into_id"],
        )

    # --- promos_merchant_canonical_map ---------------------------------------
    op.create_table(
        "promos_merchant_canonical_map",
        sa.Column(
            "promo_id",
            uuid_type(),
            sa.ForeignKey("promos.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "merchant_canonical_id",
            uuid_type(),
            sa.ForeignKey(
                "merchants_canonical.id",
                name="fk_pmcm_merchant_canonical_id",
            ),
            nullable=False,
        ),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column(
            "mapped_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column(
            "reviewed_by",
            uuid_type(),
            sa.ForeignKey("users.id", name="fk_pmcm_reviewed_by"),
            nullable=True,
        ),
        sa.Column("reviewed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "method IN ('exact','fuzzy','llm','manual')",
            name="pmcm_method_check",
        ),
    )
    op.create_index(
        "idx_pmcm_merchant_confidence",
        "promos_merchant_canonical_map",
        ["merchant_canonical_id", sa.text("confidence DESC")],
    )
    if is_postgres():
        op.create_index(
            "idx_pmcm_review_queue",
            "promos_merchant_canonical_map",
            ["method", "confidence"],
            postgresql_where=sa.text("reviewed_at IS NULL"),
        )
    else:
        op.create_index(
            "idx_pmcm_review_queue",
            "promos_merchant_canonical_map",
            ["method", "confidence"],
        )

    # --- Seed the 50 curated merchants ---------------------------------------
    bind = op.get_bind()

    for idx, (slug, name_th, name_en, mtype, alt_names, cat_default) in enumerate(
        _SEEDED_MERCHANTS, start=1
    ):
        merchant_id = _SEED_UUID_TEMPLATE.format(idx)

        if is_postgres():
            bind.execute(
                sa.text(
                    """
                    INSERT INTO merchants_canonical
                      (id, slug, display_name_th, display_name_en,
                       category_default, alt_names, merchant_type, status)
                    VALUES
                      (:id, :slug, :name_th, :name_en,
                       :cat, :alt, :mtype, 'active')
                    ON CONFLICT (id) DO NOTHING
                    """
                ),
                {
                    "id": merchant_id,
                    "slug": slug,
                    "name_th": name_th,
                    "name_en": name_en,
                    "cat": cat_default,
                    "alt": alt_names,
                    "mtype": mtype,
                },
            )
        else:
            import json as _json

            exists = bind.execute(
                sa.text("SELECT 1 FROM merchants_canonical WHERE id = :id"),
                {"id": merchant_id},
            ).scalar()
            if not exists:
                bind.execute(
                    sa.text(
                        """
                        INSERT INTO merchants_canonical
                          (id, slug, display_name_th, display_name_en,
                           category_default, alt_names, merchant_type, status)
                        VALUES
                          (:id, :slug, :name_th, :name_en,
                           :cat, :alt, :mtype, 'active')
                        """
                    ),
                    {
                        "id": merchant_id,
                        "slug": slug,
                        "name_th": name_th,
                        "name_en": name_en,
                        "cat": cat_default,
                        "alt": _json.dumps(alt_names),
                        "mtype": mtype,
                    },
                )


def downgrade() -> None:
    # Drop map table first (it FKs into merchants_canonical).
    op.drop_index(
        "idx_pmcm_review_queue",
        table_name="promos_merchant_canonical_map",
    )
    op.drop_index(
        "idx_pmcm_merchant_confidence",
        table_name="promos_merchant_canonical_map",
    )
    op.drop_table("promos_merchant_canonical_map")

    if is_postgres():
        op.drop_index(
            "idx_merchants_canonical_merged_into",
            table_name="merchants_canonical",
        )
        op.execute("DROP INDEX IF EXISTS idx_merchants_canonical_fts_gin;")
        op.execute("DROP INDEX IF EXISTS idx_merchants_canonical_altnames_gin;")
        op.drop_index(
            "idx_merchants_canonical_active",
            table_name="merchants_canonical",
        )
    else:
        op.drop_index(
            "idx_merchants_canonical_merged_into",
            table_name="merchants_canonical",
        )
        op.drop_index(
            "idx_merchants_canonical_status",
            table_name="merchants_canonical",
        )
    op.drop_index(
        "idx_merchants_canonical_slug",
        table_name="merchants_canonical",
    )
    op.drop_table("merchants_canonical")
