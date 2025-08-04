"""
src/food_scanner/bot/message_templates.py
Message Templates for Telegram Bot
Only 2 templates: success and fail
Formatted with emojis and Markdown for better UX
"""

from typing import Dict, List, Any
from ..core.constants import CARBON_FACTORS


class MessageTemplates:
    """Message templates for bot responses"""
    
    def format_product_success(self, product: Dict[str, Any]) -> str:
        """
        Format successful product lookup response
        Includes all available product information
        
        Args:
            product: Product data from database
            
        Returns:
            Formatted success message
        """
        # Extract product data with fallbacks
        name = product.get('product_name', 'Unknown Product')
        brand = product.get('brand_name', 'Unknown Brand')
        weight = product.get('weight', 0)
        unit = product.get('product_quantity_unit', 'g')
        
        # Nutrition info
        nutriscore_grade = product.get('nutriscore_grade')
        nutriscore_score = product.get('nutriscore_score')
        
        # CO2 data
        co2_per_100g = product.get('co2_total', 0)
        total_co2_impact = product.get('total_co2_impact_grams', 0)
        impact_level = product.get('impact_level', 'UNKNOWN')
        
        # Transport equivalents
        car_km = product.get('co2_vehicle_km', 0)
        train_km = product.get('co2_train_km', 0)
        bus_km = product.get('co2_bus_km', 0)
        plane_km = product.get('co2_plane_km', 0)
        
        # Format weight
        if weight >= 1000:
            weight_display = f"{weight/1000:.1f}kg"
        else:
            weight_display = f"{weight:.0f}{unit}"
        
        # Format nutriscore
        nutriscore_text = ""
        if nutriscore_grade:
            nutriscore_text = f"Nutri-Score: *{nutriscore_grade}*"
        elif nutriscore_score is not None:
            nutriscore_text = f"Nutri-Score: *{nutriscore_score}/40*"
        
        # Impact level emoji
        impact_emoji = {
            'LOW': '🟢',
            'MEDIUM': '🟡', 
            'HIGH': '🟠',
            'VERY_HIGH': '🔴'
        }.get(impact_level, '⚪')
        
        # Build the response
        response = (
            f"✅ *Product Found*\n\n"
            f"🏷️ *{name}*\n"
            f"🏭 Brand: *{brand}*\n"
            f"⚖️ Weight: *{weight_display}*\n"
        )
        
        if nutriscore_text:
            response += f"📊 {nutriscore_text}\n"
        
        response += (
            f"\n🌍 *Carbon Footprint*\n"
            f"• {co2_per_100g:.1f}g CO₂ per 100{unit}\n"
            f"• {total_co2_impact:.1f}g CO₂ total {impact_emoji} *{impact_level}*\n\n"
            f"🚗 *Transport Equivalents*\n"
            f"• 🚗 Car: *{car_km:.1f} km* ({CARBON_FACTORS['car']}g CO₂/km)\n"
            f"• 🚆 Train: *{train_km:.1f} km* ({CARBON_FACTORS['train']}g CO₂/km)\n"
            f"• 🚌 Bus: *{bus_km:.1f} km* ({CARBON_FACTORS['bus']}g CO₂/km)\n"
            f"• ✈️ Plane: *{plane_km:.2f} km* ({CARBON_FACTORS['plane']}g CO₂/km)\n\n"
            f"💡 _This chocolate has the same CO₂ impact as driving {car_km:.1f} km!_"
        )
        
        return response
    
    def format_product_not_found(self) -> str:
        """
        Format product not found response
        
        Returns:
            Formatted fail message
        """
        return (
            f"❌ *Product Not Found*\n\n"
            f"😕 Sorry, this product is not in our chocolate database.\n\n"
            f"🍫 *Our database includes:*\n"
            f"• Chocolate bars\n"
            f"• Chocolate cakes\n" 
            f"• Chocolate confectionery\n\n"
            f"💡 *Try:*\n"
            f"• Scanning a different chocolate product\n"
            f"• Checking the barcode is clearly visible\n"
            f"• Check available brands in IntegrationTestCFChannel welcome message"
        )
    
    def format_channel_welcome_message(self, brands: List[str]) -> str:
        """
        Format welcome message for IntegrationTestCFChannel
        Lists available brands and usage instructions
        
        Args:
            brands: List of available brand names
            
        Returns:
            Formatted welcome message for the channel
        """
        if not brands:
            return (
                "🍫 *Carbon Footprint Scanner - IntegrationTestCFChannel*\n\n"
                "❌ No chocolate brands available in database.\n"
                "Database is being populated..."
            )
        
        brands_text = "\n".join([f"• {brand}" for brand in sorted(brands)])
        
        return (
            f"🍫 *Welcome to Carbon Foodprint Scanner!*\n\n"
            f"📱 *How to use the bot:*\n"
            f"• Take a photo of a product barcode\n"
            f"• Or type the barcode numbers directly\n"
            f"• Get instant CO₂ impact + transport equivalents!\n\n"
            f"🏭 *Available chocolate brands ({len(brands)}):*\n"
            f"{brands_text}\n\n"
            f"🌍 *What you'll get:*\n"
            f"• Product name and brand\n"
            f"• Nutri-Score health rating\n"
            f"• CO₂ emissions per 100g and total\n"
            f"• Equivalent distances (car, train, bus, plane)\n"
            f"• Environmental impact level\n\n"
            f"💡 *Help reduce your carbon footprint - one chocolate at a time!*"
        )
    
    def format_error_message(self, error_type: str = "general") -> str:
        """
        Format error messages for different scenarios
        
        Args:
            error_type: Type of error (general, network, database)
            
        Returns:
            Formatted error message
        """
        messages = {
            "network": (
                "🔌 *Connection Error*\n\n"
                "Unable to connect to our database.\n"
                "Please try again in a few moments."
            ),
            "database": (
                "💾 *Database Error*\n\n"
                "We're experiencing technical difficulties.\n"
                "Please try again later."
            ),
            "general": (
                "⚠️ *Something went wrong*\n\n"
                "Please try again or contact support if the problem persists."
            )
        }
        
        return messages.get(error_type, messages["general"])