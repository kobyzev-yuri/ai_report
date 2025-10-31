# Quick Start: 1C Billing Integration Export

## What You Need

Export customer billing data from Oracle for 1C system integration.

**Exported fields:**
- CUSTOMER_NAME (organization or person FIO)
- AGREEMENT_NUMBER (contract number)
- ORDER_NUMBER (blank/appendix)
- CODE_1C (1C customer code)

## Run Export (1 command)

```bash
cd /mnt/ai/cnn/ai_report/oracle/export
sqlplus billing7/billing@bm7 @export_billing_integration.sql
```

**Done!** This creates:
- `billing_integration.csv` ← Import this to 1C

## File Format

Pipe-delimited CSV:
```
CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|STATUS
SUB-5397839455|300215010622150|Государственное казённое учреждение «Ресурсы Ямала»|076/18 от 01.03.2018|1|1C00123|1
```

## What's Filtered Out

✅ **Exported:**
- Active services (STATUS = 1)
- Current/working services

❌ **Excluded:**
- Clone services (`-clone-` in CONTRACT_ID)
- Inactive services (STATUS ≠ 1)
- Transferred/closed services

**Example of excluded clone:**
```
SUB-5397839455-clone-2018-05-01  ← Service transferred on 2018-05-01
```

## Need Service History?

To see all services including transfers/clones:

```bash
sqlplus billing7/billing@bm7 @export_service_history.sql
```

This shows complete audit trail of service transfers.

---

**For full docs:** See `README.md` in this directory



