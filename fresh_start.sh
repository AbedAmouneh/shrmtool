#!/bin/bash
# Fresh Start Script
# Clears deduplication memory and runs a fresh collection

# Check and backup deduplication database
if [ -f "seen_urls.db" ]; then
    mv seen_urls.db seen_urls_OLD.db
    echo "Backed up seen_urls.db to seen_urls_OLD.db"
fi

# Run fresh collection with venv Python
./venv/bin/python main_collect.py

# Completion message
echo ""
echo "✓ Fresh collection complete!"
echo "Please verify:"
echo "  - Google Sheet columns (Likes/Comments) are Numbers"
echo "  - No 'Biztoc' links exist"

