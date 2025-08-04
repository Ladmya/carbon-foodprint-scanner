# Carbon Foodprint Scanner - Main Makefile

.PHONY: help data dev bot all

# Main categories
data:
	@make -f Makefile.data help

dev:
	@make -f Makefile.dev help

bot:
	@make -f Makefile.bot help

# Quick commands (delegates to specific makefiles)
collect:
	@make -f Makefile.data collect

analyze:
	@make -f Makefile.data analyze

analyze-raw:
	@make -f Makefile.data analyze-raw

reports:
	@make -f Makefile.data reports

test-extraction:
	@make -f Makefile.data test-extraction

setup:
	@make -f Makefile.dev setup

setup-complete:
	@make -f Makefile.dev setup-complete

test:
	@make -f Makefile.dev test

run-bot:
	@make -f Makefile.bot run-bot

# Maintenance
clean:
	@make -f Makefile.dev clean
	@make -f Makefile.data clean-data

# Help
help:
	@echo "🍫 Carbon Foodprint Scanner - Main Makefile"
	@echo "=========================================="
	@echo ""
	@echo "📊 Data Operations (make data):"
	@echo "  collect         - Run data collection pipeline"
	@echo "  analyze         - Run data quality analysis"
	@echo "  analyze-raw     - Analyze raw data quality specifically"
	@echo "  reports         - Generate production reports"
	@echo "  test-extraction - Test extraction component independently"
	@echo ""
	@echo "🔧 Development (make dev):"
	@echo "  setup           - Validate environment"
	@echo "  setup-complete  - Validate complete pipeline"
	@echo "  test            - Run all tests"
	@echo "  test-unit       - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo ""
	@echo "🤖 Bot Operations (make bot):"
	@echo "  run-bot         - Start Telegram bot"
	@echo "  test-bot        - Test bot functionality"
	@echo ""
	@echo "🧹 Maintenance:"
	@echo "  clean           - Clean all temporary files"
	@echo ""
	@echo "💡 Quick commands:"
	@echo "  make collect    - Quick data collection"
	@echo "  make analyze    - Quick data analysis"
	@echo "  make analyze-raw - Quick raw data analysis"
	@echo "  make setup-complete - Quick complete validation"
	@echo "  make test       - Quick testing"
	@echo "  make run-bot    - Quick bot start"
