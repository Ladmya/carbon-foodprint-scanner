"""
src/food_scanner/bot/generate_telegram_channel_message.py
Utility script to generate welcome message for IntegrationTestCFChannel
Run this to get the formatted message to post in Telegram channel
"""

import sys
import asyncio
from pathlib import Path

# Add src to Python path
current_dir = Path(__file__).parent
src_dir = current_dir.parent.parent.parent / "src"  # Go up 3 levels to reach project root, then add src
sys.path.insert(0, str(src_dir))

from food_scanner.bot.database_service import DatabaseService
from food_scanner.bot.message_templates import MessageTemplates


async def generate_channel_welcome_message():
    """Generate welcome message for the Telegram channel"""
    
    print("🍫 Generating welcome message for IntegrationTestCFChannel...")
    print("=" * 60)
    
    try:
        # Initialize services
        db_service = DatabaseService()
        templates = MessageTemplates()
        
        # Get available brands from database
        print("📊 Fetching available brands from database...")
        brands = await db_service.get_available_brands()
        
        if not brands:
            print("⚠️  No brands found in database!")
            print("Make sure the database is populated with chocolate products.")
            return
        
        print(f"✅ Found {len(brands)} brands in database")
        
        # Generate welcome message
        welcome_message = templates.format_channel_welcome_message(brands)
        
        # Output the message
        print("\n" + "="*60)
        print("📋 COPY THIS MESSAGE TO IntegrationTestCFChannel:")
        print("="*60)
        print(welcome_message)
        print("="*60)
        
        # Also save to file
        with open("channel_welcome_message.txt", "w", encoding="utf-8") as f:
            f.write(welcome_message)
        
        print(f"\n💾 Message also saved to: channel_welcome_message.txt")
        print(f"📌 Pin this message in the IntegrationTestCFChannel!")
        
    except Exception as e:
        print(f"❌ Error generating message: {e}")
        print("Make sure the database configuration is correct.")


if __name__ == "__main__":
    asyncio.run(generate_channel_welcome_message())