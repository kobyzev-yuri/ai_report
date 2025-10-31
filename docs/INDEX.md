# Documentation Index

Complete documentation navigation for the Iridium M2M Reporting System.

## 🚀 Quick Start

| Document | Description |
|----------|-------------|
| [QUICK_START.md](QUICK_START.md) | Getting started guide - choose Oracle or PostgreSQL |
| [QUICK_REFERENCE.md](QUICK_REFERENCE.md) | Command cheat sheet for daily use |

## 🏭 Production Operations (Oracle)

| Document | Description |
|----------|-------------|
| [PRODUCTION_OPERATIONS.md](PRODUCTION_OPERATIONS.md) | **Main guide** for Oracle production operations |
| [../oracle/queries/README.md](../oracle/queries/README.md) | Production queries documentation |
| [../oracle/export/README.md](../oracle/export/README.md) | Export scripts documentation |

## 🧪 Testing Environment (PostgreSQL)

| Document | Description |
|----------|-------------|
| [../postgresql/SETUP_WITH_ORACLE_DATA.md](../postgresql/SETUP_WITH_ORACLE_DATA.md) | **Setup guide** with Oracle dump import |
| [../postgresql/export/README.md](../postgresql/export/README.md) | PostgreSQL export scripts |
| [POSTGRES_TEST_DB.md](POSTGRES_TEST_DB.md) | PostgreSQL testing database guide |

## 🏗️ Architecture & Strategy

| Document | Description |
|----------|-------------|
| [DUAL_CODEBASE_STRATEGY.md](DUAL_CODEBASE_STRATEGY.md) | **Architecture** - Oracle + PostgreSQL parallel support |
| [SIDE_BY_SIDE_COMPARISON.md](SIDE_BY_SIDE_COMPARISON.md) | Implementation comparison (Oracle vs PostgreSQL) |
| [REORGANIZATION.md](REORGANIZATION.md) | Project structure reorganization notes |

## 📤 Data Export & Integration

| Document | Description |
|----------|-------------|
| [BILLING_EXPORT_GUIDE.md](BILLING_EXPORT_GUIDE.md) | **1C integration** - Export customer billing data |
| [EXPORT_GUIDE.md](EXPORT_GUIDE.md) | General export guide |

## 📋 Technical Documentation

| Document | Description |
|----------|-------------|
| [TZ.md](TZ.md) | Technical requirements (Техническое задание) |
| [billing_integration.md](billing_integration.md) | Billing system integration details |
| [README_STREAMLIT.md](README_STREAMLIT.md) | Streamlit dashboard documentation |

## 🔧 Reference Documentation

| Document | Description |
|----------|-------------|
| [ORACLE_COMPATIBILITY_ANALYSIS.md](ORACLE_COMPATIBILITY_ANALYSIS.md) | Oracle compatibility analysis |
| [COMPATIBILITY_SUMMARY.md](COMPATIBILITY_SUMMARY.md) | Cross-platform compatibility summary |
| [STATUS.md](STATUS.md) | Project status and features |
| [SUMMARY.md](SUMMARY.md) | Project summary |
| [CHANGELOG.md](CHANGELOG.md) | Version history and changes |

---

## 📁 Directory Structure

```
ai_report/
├── README.md                          # Main project README
├── docs/                              # 📚 All documentation
│   ├── INDEX.md                       # This file
│   ├── QUICK_START.md                 # 🚀 Getting started
│   ├── PRODUCTION_OPERATIONS.md       # 🔴 Oracle operations
│   ├── DUAL_CODEBASE_STRATEGY.md     # 🏗️ Architecture
│   └── ...
│
├── oracle/                            # 🔴 PRODUCTION CODE
│   ├── queries/                       # Daily operations
│   ├── views/                         # Production views
│   ├── functions/                     # PL/SQL functions
│   └── export/                        # Export scripts
│
└── postgresql/                        # 🟢 TEST CODE
    ├── tables/                        # Test tables
    ├── views/                         # Test views
    ├── functions/                     # Test functions
    └── data/                          # Import scripts
```

---

## 🎯 Where to Start?

### For Daily Operations
👉 Start with [PRODUCTION_OPERATIONS.md](PRODUCTION_OPERATIONS.md)

### For Development/Testing
👉 Start with [QUICK_START.md](QUICK_START.md) → Choose PostgreSQL path

### For Understanding Architecture
👉 Read [DUAL_CODEBASE_STRATEGY.md](DUAL_CODEBASE_STRATEGY.md)

### For 1C Integration
👉 See [BILLING_EXPORT_GUIDE.md](BILLING_EXPORT_GUIDE.md)

---

## 📞 Support

For issues or questions:
1. Check relevant documentation above
2. See [../oracle/queries/README.md](../oracle/queries/README.md) for common queries
3. Review [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for quick commands
