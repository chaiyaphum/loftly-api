"""021 — backfill merchant descriptions + thin alt_names on seeded rows.

Migration 019 created the 50 canonical merchants but left `description_th` /
`description_en` NULL, so `/merchants/[slug]` pages ship with an empty blurb
under the merchant header. This migration populates both description columns
in Thai and English, and nudges the two rows that shipped with only a single
alt_name (`siam-discovery`, `expedia`) plus a few borderline entries so the
canonicalizer has more handles to match against.

Idempotent: each UPDATE is keyed on the deterministic seeded UUID + slug.
Re-running won't clobber admin edits on non-seeded rows, and is safe to run
after any manual tweaks to descriptions since the WHERE clause narrows to
the exact seed id set.

Downgrade nulls the descriptions back out but deliberately leaves alt_names
alone — reverting alt_names would wipe any admin additions layered on top
after this ran.

Revision ID: 021_merchant_descriptions
Revises: 020_promos_promo_type_nullable
Create Date: 2026-04-22
"""

from __future__ import annotations

import json as _json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres

revision: str = "021_merchant_descriptions"
down_revision: str | None = "020_promos_promo_type_nullable"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Stable UUID prefix matching migration 019's `_SEED_UUID_TEMPLATE`. Order
# below mirrors 019's `_SEEDED_MERCHANTS` so idx 1..50 maps to the same seed
# id slots. Tuple shape is
# (slug, description_th, description_en, alt_names_override_or_none).
_SEED_UUID_TEMPLATE = "11ff1170-0000-4000-8000-{:012d}"

_BACKFILL: list[tuple[str, str, str, list[str] | None]] = [
    (
        "starbucks",
        "เครือร้านกาแฟจากอเมริกาที่มีสาขากว่า 400 แห่งทั่วไทย · เมนูเอสเปรสโซ เฟรปปูชิโน และเบเกอรี่ จ่ายด้วยบัตรเครดิตสะสมคะแนนได้ทุกสาขา",
        "Global coffee chain with 400+ branches across Thailand. Espresso drinks, Frappuccinos, and light bakery — a staple for morning commute spend and small-ticket card earnings.",
        None,
    ),
    (
        "grab-food",
        "แอปสั่งอาหารเดลิเวอรี่ที่ครอบคลุมร้านมากที่สุดในไทย · ผูกบัตรเครดิตหมวดร้านอาหารเพื่อรับคะแนนคูณเพิ่มทุกออเดอร์",
        "Southeast Asia's dominant food-delivery app in Thailand, covering quick-service chains to hawker stalls. Ideal for stacking dining-category cashback and rewards.",
        None,
    ),
    (
        "grab-rides",
        "บริการเรียกรถและมอเตอร์ไซค์รับจ้างในกรุงเทพและหัวเมืองใหญ่ · ผูกบัตรเครดิตเพื่อชำระอัตโนมัติและเก็บคะแนนหมวดเดินทาง",
        "Ride-hailing and moto-taxi service across Bangkok and major Thai cities. Stored cards auto-charge per trip — good fit for transit-category earn rates.",
        None,
    ),
    (
        "shopee",
        "แพลตฟอร์มอีคอมเมิร์ซอันดับต้นของไทย · ครอบคลุมสินค้าทุกหมวด มีโค้ดส่วนลดจากผู้ออกบัตรเครดิตสลับลงตลอดทั้งเดือน",
        "Thailand's leading marketplace, covering everything from daily essentials to electronics. Frequently runs bank-card promo codes during Double-Day sales (9.9, 11.11, 12.12).",
        None,
    ),
    (
        "lazada",
        "แพลตฟอร์มช้อปปิ้งออนไลน์ในเครือ Alibaba · เด่นเรื่องสินค้าแบรนด์เนมบน LazMall และโค้ดบัตรเครดิตลดเพิ่มช่วงแคมเปญใหญ่",
        "Alibaba-owned online marketplace strong on LazMall brand-authorized stores. Regular bank-card codes stack on top of seller discounts during campaign days.",
        None,
    ),
    (
        "seven-eleven",
        "ร้านสะดวกซื้อที่มีสาขามากที่สุดในไทย · ของกิน ของใช้ จ่ายบิล และเติมเงิน e-wallet — หมวดที่หลายบัตรให้คะแนนพิเศษเฉพาะ",
        "Thailand's densest convenience-store chain with 14,000+ branches. Snacks, household items, bill payment, and e-wallet top-ups — a common convenience-category earn target.",
        None,
    ),
    (
        "central-department-store",
        "ห้างสรรพสินค้าหลักของเครือเซ็นทรัล · แฟชั่น บิวตี้ และของใช้ในบ้าน มักมีโปรโมชั่นบัตรเครดิตสะสมคะแนน The 1 ร่วมกับบัตรพาร์ทเนอร์",
        "Flagship department-store chain of the Central Group spanning fashion, beauty, and home. Frequent cross-promotions with Central The 1 loyalty + partner bank cards.",
        None,
    ),
    (
        "central-restaurants-group",
        "กลุ่มร้านอาหารในเครือเซ็นทรัล ครอบคลุม Mister Donut, Auntie Anne's, KFC บางสาขาและอีกหลายแบรนด์ · รับโปรบัตรเครดิตข้ามแบรนด์ได้",
        "Restaurant operator under Central Group, running Mister Donut, Auntie Anne's, select KFC franchises, and more. Cross-brand card promos apply across the portfolio.",
        ["CRG", "Central Restaurants", "เซ็นทรัล เรสเตอรองส์"],
    ),
    (
        "siam-paragon",
        "ศูนย์การค้าระดับลักซ์ชัวรีใจกลางสยาม · รวมแบรนด์ไฮเอนด์ ร้านอาหารชั้นนำ และ Gourmet Market — จุดใช้จ่ายหลักของบัตรพรีเมียม",
        "Luxury mall in the heart of Siam with high-end fashion, fine-dining, and Gourmet Market. A primary spending venue for premium and signature-tier cards.",
        ["Paragon", "สยามพารากอน", "พารากอน"],
    ),
    (
        "siam-discovery",
        "ศูนย์การค้าไลฟ์สไตล์แนว design-led ในย่านสยาม · แฟชั่นดีไซเนอร์ ของตกแต่งบ้าน และคาเฟ่ เหมาะกับการรูดบัตรหมวดไลฟ์สไตล์",
        "Lifestyle-focused mall in the Siam district with designer fashion, home decor, and cafes. A solid venue for lifestyle-category card spend.",
        ["Siam Discovery", "สยามดิสคัฟเวอรี่", "ดิสคัฟเวอรี่"],
    ),
    (
        "iconsiam",
        "ศูนย์การค้าริมแม่น้ำเจ้าพระยา · รวม Apple Store สาขาแรกในไทย, สยามทาคาชิมายะ และ SookSiam · จุดหมายของบัตรเครดิตไลฟ์สไตล์พรีเมียม",
        "Riverside mega-mall featuring Thailand's first Apple Store, Siam Takashimaya, and the SookSiam floating-market floor. A magnet for premium lifestyle card spend.",
        ["Icon Siam", "ไอคอนสยาม", "ไอคอน สยาม"],
    ),
    (
        "foodpanda",
        "แอปสั่งอาหารคู่แข่งของ Grab Food · ครอบคลุมร้านมากในทุกจังหวัดใหญ่ มีโปร pandapro และส่วนลดจับคู่บัตรเครดิตเป็นประจำ",
        "Grab Food's main delivery rival in Thailand with strong coverage in secondary cities. Regular pandapro and bank-card promo codes make it a frequent dining-spend channel.",
        None,
    ),
    (
        "agoda",
        "แพลตฟอร์มจองโรงแรมและตั๋วเครื่องบินที่ก่อตั้งในไทย · ครอบคลุมโรงแรมทั่วเอเชีย เหมาะกับบัตรเครดิตสายท่องเที่ยวเก็บไมล์",
        "Thai-founded global travel booking site covering hotels, flights, and packages across Asia. Popular choice for weekend trips paid with travel-tier credit cards.",
        ["agoda.com", "อโกด้า", "Agoda TH"],
    ),
    (
        "booking-com",
        "แพลตฟอร์มจองที่พักระดับโลก · เหมาะกับจองโรงแรมต่างประเทศ บัตรเครดิตสกุลเงินต่างประเทศหลายใบให้คะแนนเพิ่มหมวดท่องเที่ยว",
        "Global hotel booking platform covering international stays end-to-end. Several foreign-spend credit cards pay bonus earn on Booking.com transactions.",
        ["Booking", "booking.com", "บุ๊กกิ้ง"],
    ),
    (
        "expedia",
        "แพลตฟอร์มจองทริปครบวงจร ทั้งเที่ยวบิน โรงแรม และรถเช่า · แพ็กเกจแบบ Bundle มักได้ราคาดีและเก็บคะแนนบัตรท่องเที่ยวได้เต็ม",
        "Full-service travel booking for flights, hotels, and car rentals. Bundle deals commonly unlock better net pricing, and travel-card bonus categories apply.",
        ["Expedia TH", "expedia.co.th", "เอ็กซ์พีเดีย"],
    ),
    (
        "bts",
        "ระบบรถไฟฟ้าลอยฟ้าสายหลักของกรุงเทพ · เติม Rabbit Card ด้วยบัตรเครดิตเพื่อเก็บคะแนนหมวดขนส่งสาธารณะขณะเดินทาง",
        "Bangkok's flagship elevated rail system. Rabbit Card top-ups via credit card earn transit-category points on daily commutes.",
        None,
    ),
    (
        "mrt",
        "ระบบรถไฟฟ้าใต้ดินและสายสีต่าง ๆ ของกรุงเทพ · เติมบัตร MRT Plus ผ่านบัตรเครดิตเพื่อสะสมคะแนนหมวดเดินทาง",
        "Bangkok's subway and extended-line network. MRT Plus card top-ups via credit card qualify for transit-category earn on most travel-tier cards.",
        None,
    ),
    (
        "tops-supermarket",
        "ซูเปอร์มาร์เก็ตในเครือเซ็นทรัล · ของสด ของใช้ในบ้าน และสินค้านำเข้า · บัตร Central The 1 ให้คะแนนคูณเพิ่มหมวดซูเปอร์มาร์เก็ต",
        "Supermarket chain under Central Group carrying fresh produce, household goods, and imports. Central The 1 cards earn bonus points on grocery-category spend here.",
        None,
    ),
    (
        "makro",
        "ร้านค้าส่งขนาดใหญ่สำหรับผู้ประกอบการและครัวเรือน · ซื้อปริมาณมากราคาถูก · รายการยอดสูงเหมาะกับบัตรที่ให้คะแนนหมวด groceries",
        "Wholesale warehouse store for businesses and bulk-buying households. High-ticket baskets pair well with grocery-category earn cards.",
        ["Siam Makro", "แม็คโคร", "แมคโคร"],
    ),
    (
        "big-c",
        "ไฮเปอร์มาร์เก็ตเครือ BJC · อาหารสด เสื้อผ้า เครื่องใช้ไฟฟ้า · โปรบัตรเครดิตสลับกันทุกเดือนที่เช็คเอาท์",
        "BJC-owned hypermarket chain covering groceries, apparel, and electronics. Frequent rotating bank-card promos at checkout.",
        None,
    ),
    (
        "lotuss",
        "ไฮเปอร์มาร์เก็ตในเครือ CP (ชื่อเดิม Tesco Lotus) · ของสด สินค้าอุปโภคบริโภค และคลังสินค้าขนาดใหญ่นอกเมือง",
        "CP-owned hypermarket chain (formerly Tesco Lotus) offering groceries, household goods, and large out-of-town superstores.",
        None,
    ),
    (
        "cp-fresh-mart",
        "ร้านค้าปลีกขนาดเล็กเน้นเนื้อสัตว์และอาหารสดของเครือ CP · สาขาในย่านที่อยู่อาศัย สะดวกสำหรับซื้อประจำวัน",
        "Neighborhood-scale retail concept from CP focused on fresh meat and ready ingredients. Convenient daily-shop format in residential areas.",
        ["CP Freshmart", "ซีพี เฟรชมาร์ท", "CP Fresh"],
    ),
    (
        "villa-market",
        "ซูเปอร์มาร์เก็ตพรีเมียมเน้นสินค้านำเข้าและอาหารชาวต่างชาติ · สาขาในย่าน expat หลายแห่ง · บิลเฉลี่ยสูงเหมาะกับบัตร dining/groceries tier สูง",
        "Premium supermarket chain specializing in imported goods and expat staples. Expat-dense branch footprint with higher-than-average basket sizes.",
        ["Villa", "วิลล่ามาร์เก็ต", "Villa Supermarket"],
    ),
    (
        "gourmet-market",
        "ซูเปอร์มาร์เก็ตระดับพรีเมียมในห้างเครือ The Mall · ของสด premium, ของนำเข้า และเดลี่ · บัตร M Card + co-brand ได้คะแนนคูณ",
        "Premium supermarket inside The Mall Group malls, stocking imports, fine produce, and a deli counter. M Card and co-brand cards earn bonus rewards.",
        ["Gourmet", "กูร์เมต์", "Gourmet Market The Mall"],
    ),
    (
        "terminal-21",
        "ศูนย์การค้าธีมเมืองต่างประเทศ (อโศก, พระราม 3, พัทยา, โคราช) · ร้านค้า SME และฟู้ดคอร์ทราคาเข้าถึงได้ — เหมาะกับบัตรเริ่มต้น",
        "Themed shopping malls styled after world cities (Asok, Rama 3, Pattaya, Korat). Heavy SME tenant mix and affordable food courts — good fit for starter-tier cards.",
        ["Terminal21", "เทอร์มินอล21", "T21"],
    ),
    (
        "mbk-center",
        "ห้างย่านปทุมวันที่ขึ้นชื่อเรื่องมือถือ แฟชั่น และของฝาก · ราคาต่อรองได้บางร้าน แต่ส่วนใหญ่รูดบัตรเครดิตได้ตามปกติ",
        "Pathum Wan landmark mall known for mobile phones, fashion, and souvenirs. Bargaining is common at some stalls, though most tenants accept standard card payments.",
        None,
    ),
    (
        "the-mall",
        "ศูนย์การค้าเครือ The Mall (บางกะปิ งามวงศ์วาน ท่าพระ โคราช) · แฟชั่น อาหาร และ Gourmet Market · รองรับ M Card และบัตรร่วม",
        "The Mall Group's shopping-center brand (Bangkapi, Ngamwongwan, Tha Phra, Korat). Fashion, dining, and Gourmet Market anchor — supports M Card and co-brand earn.",
        ["The Mall Group", "เดอะมอลล์", "The Mall Shopping Center"],
    ),
    (
        "emporium",
        "ศูนย์การค้าคู่แฝดย่านพร้อมพงษ์ · แบรนด์ลักซ์ชัวรี ร้านอาหารระดับพรีเมียม และ Gourmet Market · จุดใช้จ่ายของบัตรพรีเมียมและ Signature",
        "Twin luxury malls in the Phrom Phong district anchored by high-end fashion, upscale dining, and Gourmet Market — a core spend venue for premium and signature cards.",
        None,
    ),
    (
        "robinson",
        "ห้างสรรพสินค้าเครือเซ็นทรัล เน้นราคาเข้าถึงได้และสาขาครอบคลุมทุกภูมิภาค · รับบัตร The 1 สะสมคะแนนข้ามร้านเซ็นทรัลได้",
        "Central Group's mid-market department-store format with nationwide reach. Earns The 1 points that stack across the broader Central ecosystem.",
        ["Robinsons", "โรบินสัน", "Robinson Lifestyle"],
    ),
    (
        "esso",
        "เครือสถานีบริการน้ำมันในไทย · ชำระด้วยบัตรเครดิตหมวดน้ำมันเพื่อรับเครดิตเงินคืนหรือคะแนนคูณ · บางสาขามี Tesco/7-11 ในปั๊ม",
        "Petrol-station chain across Thailand. Paying by fuel-category credit card typically earns cashback or bonus points; select stations have co-located convenience stores.",
        ["Esso Thailand", "เอสโซ่", "Esso Station"],
    ),
    (
        "ptt-station",
        "สถานีบริการน้ำมันรายใหญ่ของ ปตท. · เติมน้ำมัน ซื้อ Cafe Amazon และ 7-Eleven ในปั๊ม · คะแนน Blue Card แลกได้กับบัตรเครดิต co-brand",
        "PTT's flagship fuel-station network bundling Cafe Amazon and 7-Eleven on-site. Blue Card loyalty combines with several co-brand credit cards.",
        None,
    ),
    (
        "shell",
        "เครือสถานีบริการน้ำมัน Shell · โปรแกรม Shell ClubSmart และพาร์ทเนอร์บัตรเครดิตหลายใบให้คะแนนคูณหมวดน้ำมัน",
        "Shell petrol-station network with the Shell ClubSmart loyalty program. Multiple bank cards apply fuel-category multipliers to purchases here.",
        ["Shell Thailand", "เชลล์", "Shell V-Power"],
    ),
    (
        "bangchak",
        "เครือสถานีบริการน้ำมันสัญชาติไทย · สมาชิก Bangchak member สะสมคะแนนทุกลิตร และโปรบัตรเครดิตรายเดือนจับคู่เพิ่ม",
        "Thai-owned petrol-station chain. The Bangchak member program earns per-litre points and stacks with rotating monthly credit-card promos.",
        ["Bangchak Petroleum", "บางจาก", "ปั๊มบางจาก"],
    ),
    (
        "true-coffee",
        "เครือคาเฟ่ของกลุ่ม True · สาขาใน True Digital Park, True Shop และห้างพันธมิตร · บัตร True Card คูณคะแนนในเครือ dtac/True",
        "True Group's in-house cafe chain, located inside True Digital Park, True Shops, and partner malls. True Card cards multiply points inside the dtac/True ecosystem.",
        ["TrueCoffee", "ทรูคอฟฟี่", "True Coffee Go"],
    ),
    (
        "amazon-cafe",
        "คาเฟ่ของ OR (ปตท.) สาขาเยอะที่สุดในประเทศ · กาแฟราคาเข้าถึงได้พบในปั๊ม ปตท., ห้าง, และโรงพยาบาล · นิยมจ่ายด้วย Blue Card + เครดิต",
        "OR (PTT)-operated cafe chain — the country's largest by store count, found at PTT stations, malls, and hospitals. Commonly paid with Blue Card plus a co-brand credit card.",
        None,
    ),
    (
        "au-bon-pain",
        "ร้านเบเกอรี่แบบฝรั่งเศส-อเมริกัน · ครัวซองต์ แซนด์วิช และกาแฟ · สาขาในย่าน CBD เหมาะกับมื้อเช้าและมื้อเที่ยงบัตรเครดิต dining",
        "French-American bakery cafe chain with croissants, sandwiches, and coffee. CBD-heavy footprint makes it a common breakfast/lunch dining-category spend.",
        ["ABP", "โอบองแปง", "Au Bon Pain Thailand"],
    ),
    (
        "kfc",
        "เครือร้านไก่ทอดอเมริกันที่เป็น QSR อันดับต้นของไทย · ดำเนินการโดย CRG, ยัม, และ RD บริษัทบัตร co-brand ต่างให้โปรเป็นระยะ",
        "America's fried-chicken QSR — among Thailand's top quick-service chains, operated by CRG, Yum, and RD with rotating co-brand card promos.",
        ["KFC Thailand", "เคเอฟซี", "KFC TH"],
    ),
    (
        "mcdonalds",
        "เครือฟาสต์ฟู้ดอเมริกัน · เปิด 24 ชั่วโมงหลายสาขา · เดลิเวอรีผ่าน McDelivery และ Grab — เข้าหมวด dining ของบัตรเครดิตส่วนใหญ่",
        "American fast-food chain, many locations open 24 hours. McDelivery and Grab Food orders still qualify for dining-category earn on most credit cards.",
        ["McDonald", "McD", "แมคโดนัลด์", "McDonalds"],
    ),
    (
        "pizza-hut",
        "เครือร้านพิซซ่าสัญชาติอเมริกัน · สั่งเดลิเวอรีและนั่งทาน · โปรโค้ด 1150 และบัตรเครดิตพาร์ทเนอร์ลดเพิ่มช่วงสิ้นเดือน",
        "American pizza chain with dine-in and delivery. Promo code 1150 and partner-bank card discounts frequently stack near month-end campaigns.",
        ["Pizza Hut Thailand", "พิซซ่าฮัท", "Pizza Hut TH"],
    ),
    (
        "the-pizza-company",
        "แบรนด์พิซซ่าท้องถิ่นของไทยเครือ Minor Food · สั่ง 1112 เดลิเวอรีได้ทั่วประเทศ · บัตรเครดิต co-brand ลดเพิ่มเป็นประจำ",
        "Thailand's home-grown pizza chain under Minor Food. Nationwide 1112 delivery, and co-brand credit cards regularly discount online and in-store orders.",
        ["Pizza Company", "เดอะพิซซ่าคอมปะนี", "TPC"],
    ),
    (
        "mk-suki",
        "เครือร้านสุกี้ยอดนิยมของไทย · น้ำจิ้มสูตรเฉพาะและเมนูผัก-เนื้อ-ติ่มซำ · โปรวันพฤหัสลดเพิ่มและโปรบัตรเครดิตพาร์ทเนอร์",
        "Thailand's go-to Thai-style suki (hot-pot) chain with signature dipping sauce and vegetables/meat/dim-sum menu. Thursday promos and partner-bank card deals run regularly.",
        None,
    ),
    (
        "fuji-restaurant",
        "ร้านอาหารญี่ปุ่นสัญชาติไทย · ซูชิ เทมปุระ และเซ็ตมื้อเที่ยงราคาเข้าถึงได้ · มีโปรร่วมกับบัตรเครดิตหลายธนาคาร",
        "Thai-owned Japanese casual-dining chain with sushi, tempura, and affordable lunch sets. Runs cross-bank credit-card promotions year-round.",
        ["Fuji", "ฟูจิเรสเตอรองต์", "Fuji Japanese Restaurant"],
    ),
    (
        "coca-suki",
        "ร้านสุกี้-อาหารจีนสไตล์ไทยรุ่นเก๋า ก่อตั้งตั้งแต่ พ.ศ. 2500 · เหมาะสำหรับมื้อครอบครัว บัตรเครดิตหมวด dining ใช้ได้เต็ม",
        "Heritage suki and Chinese-Thai restaurant chain founded in 1957. A classic family-dining choice where dining-category credit-card earn applies in full.",
        ["Coca", "โคคา", "Coca Restaurant"],
    ),
    (
        "bar-b-q-plaza",
        "เครือร้านปิ้งย่างสไตล์มงกุฎของไทย · มีเซ็ตบุฟเฟ่ต์และอะลาคาร์ต · โปรบัตรเครดิต (KBank, SCB, KTC) หมุนเวียนเป็นประจำ",
        "Thailand's signature Mongolian-style grill chain with both buffet and a la carte menus. KBank, SCB, and KTC promos rotate through month-to-month.",
        None,
    ),
    (
        "sukishi",
        "เครือบุฟเฟ่ต์ปิ้งย่างสไตล์เกาหลี-ญี่ปุ่น · เมนูเนื้อวัวพรีเมียมและข้าวปั้น · โปรบัตร ONE Card และบัตรเครดิตคู่สัญญาลดได้",
        "Korean-Japanese grill-buffet chain with premium beef cuts and onigiri. Supports ONE Card loyalty plus contracted credit-card discount programs.",
        ["Sukishi Korean Charcoal Grill", "ซูกิชิ", "Sukishi Buffet"],
    ),
    (
        "oishi",
        "เครือร้านอาหารญี่ปุ่นของกลุ่มไทยเบฟ · บุฟเฟ่ต์ชาบูชาบู ซูชิและราเมง · ราคาคุ้มและมีโปรจับคู่บัตรเครดิตทุกเดือน",
        "ThaiBev-owned Japanese restaurant group running shabu buffet, sushi, and ramen concepts. Value pricing with monthly credit-card pairings.",
        ["Oishi Group", "โออิชิ", "Oishi Buffet"],
    ),
    (
        "jim-thompson",
        "แบรนด์ผ้าไหมและโฮมแวร์สัญชาติไทยที่มีชื่อเสียงระดับสากล · สาขาที่ outlet ราชเทวีและสยามพารากอน · เหมาะกับบัตรลักซ์ชัวรีและของฝาก",
        "Internationally recognized Thai silk and home-goods brand with outlets at Ratchathewi and Siam Paragon. A common luxury and souvenir purchase venue.",
        ["Jim Thompson Thai Silk", "จิมทอมป์สัน", "JT"],
    ),
    (
        "muji-thailand",
        "แบรนด์ไลฟ์สไตล์จากญี่ปุ่น · เสื้อผ้า เครื่องเขียน และของใช้ในบ้านดีไซน์มินิมอล · สมาชิก MUJI passport สะสมคะแนนร่วมกับบัตรเครดิตได้",
        "Japanese minimalist lifestyle brand covering apparel, stationery, and home goods. MUJI passport loyalty stacks with credit-card rewards.",
        ["MUJI", "มูจิ", "Muji TH"],
    ),
    (
        "uniqlo-thailand",
        "แบรนด์เครื่องแต่งกาย basics จากญี่ปุ่น · เสื้อ HEATTECH, AIRism และ LifeWear · โปรร่วมกับบัตรเครดิตบางใบลดเพิ่มในช่วงเปิดภาค",
        "Japanese basics apparel brand known for HEATTECH, AIRism, and LifeWear. Back-to-season sales often stack additional credit-card discounts.",
        ["UNIQLO", "ยูนิโคล่", "Uniqlo TH"],
    ),
    (
        "hm-thailand",
        "แบรนด์ fast-fashion สัญชาติสวีเดน · คอลเลกชันหมุนทุกเดือนและร่วมมือกับดีไซเนอร์เป็นระยะ · สมาชิก H&M Member + บัตรเครดิตสะสมได้",
        "Swedish fast-fashion brand with rotating monthly collections and periodic designer collaborations. H&M Member perks combine with credit-card rewards.",
        None,
    ),
]


def upgrade() -> None:
    bind = op.get_bind()
    now_fn = "now()" if is_postgres() else "CURRENT_TIMESTAMP"

    for idx, (slug, desc_th, desc_en, alt_override) in enumerate(_BACKFILL, start=1):
        merchant_id = _SEED_UUID_TEMPLATE.format(idx)

        if alt_override is not None:
            if is_postgres():
                bind.execute(
                    sa.text(
                        f"""
                        UPDATE merchants_canonical
                           SET description_th = :desc_th,
                               description_en = :desc_en,
                               alt_names      = :alt,
                               updated_at     = {now_fn}
                         WHERE id = :id AND slug = :slug
                        """
                    ),
                    {
                        "id": merchant_id,
                        "slug": slug,
                        "desc_th": desc_th,
                        "desc_en": desc_en,
                        "alt": alt_override,
                    },
                )
            else:
                bind.execute(
                    sa.text(
                        f"""
                        UPDATE merchants_canonical
                           SET description_th = :desc_th,
                               description_en = :desc_en,
                               alt_names      = :alt,
                               updated_at     = {now_fn}
                         WHERE id = :id AND slug = :slug
                        """
                    ),
                    {
                        "id": merchant_id,
                        "slug": slug,
                        "desc_th": desc_th,
                        "desc_en": desc_en,
                        "alt": _json.dumps(alt_override),
                    },
                )
        else:
            bind.execute(
                sa.text(
                    f"""
                    UPDATE merchants_canonical
                       SET description_th = :desc_th,
                           description_en = :desc_en,
                           updated_at     = {now_fn}
                     WHERE id = :id AND slug = :slug
                    """
                ),
                {
                    "id": merchant_id,
                    "slug": slug,
                    "desc_th": desc_th,
                    "desc_en": desc_en,
                },
            )


def downgrade() -> None:
    # Null-out descriptions on the seeded id set; leave alt_names alone because
    # reverting them could clobber admin edits layered on top after this ran.
    bind = op.get_bind()
    for idx in range(1, len(_BACKFILL) + 1):
        merchant_id = _SEED_UUID_TEMPLATE.format(idx)
        bind.execute(
            sa.text(
                """
                UPDATE merchants_canonical
                   SET description_th = NULL,
                       description_en = NULL
                 WHERE id = :id
                """
            ),
            {"id": merchant_id},
        )
