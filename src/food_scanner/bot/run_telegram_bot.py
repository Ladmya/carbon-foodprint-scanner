"""
carbon_foodprint/src/food_scanner/bot/run_telegram_bot.py
Launch script for Carbon Foodprint Scanner Telegram Bot
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Add src to Python path
current_dir = Path(__file__).parent
src_dir = current_dir.parent.parent.parent / "src"  # Go up 3 levels to reach project root, then add src
sys.path.insert(0, str(src_dir))

from food_scanner.core.config import telegram_test_bot_token, supabase_test_db_url, supabase_test_db_service_key
from food_scanner.bot.database_service import DatabaseService
from food_scanner.bot.message_templates import MessageTemplates
from food_scanner.bot.barcode_scanner import BarcodeScanner

from telegram.ext import Application, MessageHandler, filters

# Services globaux pour les handlers
db_service = DatabaseService()
templates = MessageTemplates()
scanner = BarcodeScanner()

def setup_logging():
    """Configure logging for the bot with proper file organization"""
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create handlers
    bot_handler = RotatingFileHandler(
        logs_dir / "bot.log",
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    bot_handler.setLevel(logging.INFO)
    
    error_handler = RotatingFileHandler(
        logs_dir / "error.log",
        maxBytes=5*1024*1024,   # 5MB
        backupCount=3,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    bot_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(bot_handler)
    root_logger.addHandler(error_handler)
    root_logger.addHandler(console_handler)


def main():
    """Main entry point for the bot"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Check for required environment variables    
    if not telegram_test_bot_token:
        logger.error("TELEGRAM_BOT_TOKEN environment variable is required")
        logger.info("Please set your Telegram bot token:")
        logger.info("export TELEGRAM_BOT_TOKEN='your_token_here'")
        sys.exit(1)
    
    # Check database configuration    
    if not supabase_test_db_url or not supabase_test_db_service_key:
        logger.error("Supabase configuration missing")
        logger.info("Please set your Supabase credentials:")
        logger.info("export SUPABASE_TEST_DATABASE_URL='your_url_here'")
        logger.info("export SUPABASE_TEST_DATABASE_SERVICE_KEY='your_key_here'")
        sys.exit(1)
    
    # Create and run bot
    logger.info("🍫 Starting Carbon Foodprint Scanner Bot...")
    #Direct setup of the bot no wrapper class
    bot = Application.builder().token(telegram_test_bot_token).build()
    
    # Handlers fonction class method for channel + private chat
    async def handle_photo(update, context):
        try:
            # Handle both private messages and channel posts
            message = update.message or update.channel_post
            if not message or not message.photo:
                await message.reply_text("❌ No photo found in message.")
                return
            
            photo_file = await message.photo[-1].get_file()
            photo_bytes = await photo_file.download_as_bytearray()
        
            barcode = scanner.extract_barcode_from_image(photo_bytes)
        
            if not barcode:
                await message.reply_text(
                    "❌ No barcode detected in the image. Please ensure the barcode is clearly visible and try again."
                )
                return
        
            await process_barcode(message, barcode)
        
        except Exception as e:
            logger.error(f"Error processing photo: {e}")
            if message:
                await message.reply_text("❌ Error processing image. Please try again.")

    async def handle_text(update, context):
        message = update.message or update.channel_post
        if not message:
            return
        
        barcode = message.text.strip()
    
        if not barcode.isdigit() or len(barcode) < 8 or len(barcode) > 18:
            await message.reply_text(
                "❌ Invalid barcode format. Please send a valid barcode (8-18 digits) or take a photo."
            )
            return
        
        await process_barcode(message, barcode)

    async def process_barcode(message, barcode):
        try:
            product = await db_service.get_product_by_barcode(barcode)
        
            if product:
                response = templates.format_product_success(product)
                await message.reply_text(response, parse_mode='Markdown')
                logger.info(f"Product found: {barcode} - {product.get('product_name', 'Unknown')}")
            else:
                response = templates.format_product_not_found()
                await message.reply_text(response, parse_mode='Markdown')
                logger.info(f"Product not found: {barcode}")
            
        except Exception as e:
            logger.error(f"Error processing barcode {barcode}: {e}")
            await message.reply_text("❌ Database error. Please try again later.")

    # Handlers for private messages
    bot.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    bot.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Handlers for channels
    bot.add_handler(MessageHandler(filters.PHOTO & filters.UpdateType.CHANNEL_POST, handle_photo))  
    bot.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.UpdateType.CHANNEL_POST, handle_text))
    
    # Run bot - SYNCHRONOUS VERSION  handling its own event loop
    logger.info("Bot ready! Send a barcode photo or type a barcode number.")
    bot.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    # Quick environment check
    print("🍫 Carbon Foodprint Scanner Bot")
    print("=" * 40)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required")
        sys.exit(1)
    
    # Check required packages
    try:
        import telegram
        import PIL
        import pyzbar
        print("✅ All required packages installed")
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        print("Please install: pip install python-telegram-bot Pillow pyzbar")
        sys.exit(1)
    
    # Run the bot
    main()