*Report generated at UTC HOUR*: 00:00 UTC

* Urgent Action Required*
• *Payments_Layout_1_V3 (id: 220504)* – 2025-09-08: 50 files with critical volume anomalies — Clube up 103% (2,830,173), ApplePay down 56%, Beneficios up 114,200%, Shop up 215%, Saipos up 469%, CBK up 100%, WhiteLabel -100%, Anota-ai up 339% — all well outside normal patterns → *Action:* Confirm coverage/window; monitor next run
• *Payments_Layout_2_V3 (id: 220505)* – 2025-09-08: BR_MVP_payments_accounting_report: 36 files with sustained volume drops of 52–78% (e.g., 252,830 vs 525,930 rows, -51.9%) — below 95% bound 28,055–474,404; 8 failed files (schema mismatch), 2 duplicates (processing blocked) → *Action:* Investigate root cause for BR_MVP_payments_accounting_report volume drops and schema failures; review upstream systems and recent changes; resolve schema mismatches and duplicate file issues; validate downstream completeness and timeliness; escalate to data engineering if persistent.
• *Payments_Layout_3_V3 (id: 220506)* – 2025-09-08: 79 files with volume increases of 400%–2,800% since 2025-03-03 — entity: _BR_3DS_payments_accounting_report (e.g., 1,652,248 vs 70,995; 2,081,276 vs 70,995) → *Action:* Confirm root cause of sustained volume surge with data engineering and business operations; review upstream changes and business events since March 2025; escalate for immediate investigation.
• *WuPay_Sale payments_2 (id: 228036)* – 2025-09-16: 2 files missing past 20:00–20:30 UTC window — entities: anota-ai, _; 4 files blocked as duplicates, 3 failed (schema mismatch), 2 deleted → *Action:* Notify provider to generate/re-send; re-run ingestion and verify completeness
• *WuPay_STL adjustments_3 (id: 239613)* – 2025-09-08: 37 files duplicated and failed (status = 'stopped') — processing blocked for 18.5% of files → *Action:* Notify provider to resolve duplication; re-run ingestion and verify completeness
• *Soop Transaction PIX 3 (id: 199944)* – 2025-08-19: 2 duplicated, empty files blocked from processing — files: 2_Soop_CPIX_20250819_M___PIX_78AFAE04362B40899DAEE1B6EF3624D7_14380200000121_0002.csv, 2_Soop_CPIX_20250819_M___PIX_78AFAE04362B40899DAEE1B6EF3624D7_14380200000121_0001.csv → *Action:* Notify provider to investigate and resolve duplication and upstream data gap; reprocess files after correction

* Needs Attention*
• *Desco PIX (id: 209773)* – 2025-09-08: 8 files unexpectedly empty (0 rows) — abnormal for Desco PIX (historically rare, median ~146,000 rows); last 5 files uploaded significantly early (~10:11–10:20-05:00 vs expected 15:10–15:25 UTC) → *Action:* Investigate the upstream data generation and ingestion process for the listed empty files. If this pattern continues, escalate as a potential data loss or system failure event.
• *MyPal_Activity report (id: 195439)* – 2025-09-08: 7 files uploaded significantly late (24–168 hours delay vs. expected 02:00 UTC D+3 schedule) — e.g., activity_report_bc5nbnhgs8zg5z7t_2025-09-05.csv uploaded 168 hours late → *Action:* Confirm schedule change; adjust downstream triggers if needed
• *Itm Pagamentos (id: 224602)* – 2025-08-25: ItmLancamentos_pagamento volume 192,278 (last week: 586,930; -67.2%) → *Action:* Confirm coverage/window; monitor next run
• *Itm Devolução (id: 224603)* – 2025-09-08: 8 files with volume variations 52–87% from baseline — e.g., 9,992 (−52.3%) to 39,157 (+86.9%) vs last week 20,948; outside normal day-of-week ranges → *Action:* Confirm coverage/window; monitor next run
• *WuPay_Sale_adjustments_3 (id: 239611)* – 2025-09-08: 6 files uploaded 48–96 hours late — both merchants ('_' and 'anota-ai'), far outside 20:00 UTC window; 6 files detected as duplicates (processing stopped) → *Action:* Validate downstream completed; track if persists

* No Action Needed*
• *Settlement_Layout_2 (id: 195385)* – 2025-09-08: `[200] records`
• *MyPal_DBR RX (id: 195436)* – 2025-09-08: `[14,062,600] records`
• *Settlement_Layout_1 (id: 196125)* – 2025-09-06: `[4,777,110] records`
• *Soop - Tipo 2 (id: 207936)* – 2025-08-12: `[200] records`
• *Soop - Tipo 3 (id: 207938)* – 2025-09-08: `[451,908] records`
• *Desco Devoluções (id: 211544)* – 2025-09-08: `[950,314] records`
• *WuPay_STL payments_2 (id: 228038)* – 2025-09-08: `[200] records`