#!/bin/bash
# ============================================================================
# Compare Oracle and PostgreSQL View Results
# Verifies both databases produce identical outputs
# ============================================================================

set -e

ORACLE_DIR="/mnt/ai/cnn/ai_report/oracle/export"
POSTGRES_DIR="/mnt/ai/cnn/ai_report/postgresql/export"

echo "============================================================================"
echo "Oracle vs PostgreSQL Comparison"
echo "============================================================================"
echo ""

# Check if export files exist
if [ ! -f "$ORACLE_DIR/oracle_function_tests.txt" ]; then
    echo "❌ Oracle export files not found!"
    echo "   Run: cd $ORACLE_DIR && sqlplus billing7/billing@bm7 @export_view_results.sql"
    exit 1
fi

if [ ! -f "$POSTGRES_DIR/postgres_function_tests.txt" ]; then
    echo "❌ PostgreSQL export files not found!"
    echo "   Run: cd $POSTGRES_DIR && psql -U postgres -d billing -f export_view_results.sql"
    exit 1
fi

echo "✓ Export files found"
echo ""

# Files to compare
FILES=(
    "function_tests"
    "v_spnet_overage_analysis"
    "v_consolidated_overage_report"
    "v_iridium_services_info"
    "v_consolidated_report_with_billing"
)

PASS=0
FAIL=0

for file in "${FILES[@]}"; do
    ORACLE_FILE="$ORACLE_DIR/oracle_${file}.txt"
    POSTGRES_FILE="$POSTGRES_DIR/postgres_${file}.txt"
    
    echo "Comparing: $file"
    
    # Extract numeric data only (ignore formatting differences)
    if diff -w -B "$ORACLE_FILE" "$POSTGRES_FILE" > /dev/null 2>&1; then
        echo "  ✓ EXACT MATCH"
        ((PASS++))
    else
        echo "  ⚠ DIFFERENCES FOUND"
        ((FAIL++))
        
        # Show first few differences
        echo "  First differences:"
        diff -u "$ORACLE_FILE" "$POSTGRES_FILE" | head -30 | sed 's/^/    /'
        echo ""
    fi
done

echo ""
echo "============================================================================"
echo "Summary"
echo "============================================================================"
echo "Passed: $PASS"
echo "Failed: $FAIL"
echo ""

if [ $FAIL -eq 0 ]; then
    echo "✓ All comparisons passed! Databases are in sync."
    exit 0
else
    echo "⚠ Some differences found. Review output above."
    echo ""
    echo "Note: Minor formatting differences are acceptable."
    echo "Focus on:"
    echo "  - Record counts should match"
    echo "  - Numeric calculations should be identical"
    echo "  - Function tests should all show PASS"
    exit 1
fi


