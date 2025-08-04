"""
src/food_scanner/bot/barcode_scanner.py
Barcode Scanner using Pyzbar
Extracts barcodes from images sent via Telegram
Handles various image formats and barcode types
"""

import logging
from typing import Optional
from io import BytesIO
from PIL import Image
from pyzbar import pyzbar
from ..core.constants import DATA_QUALITY_THRESHOLDS

logger = logging.getLogger(__name__)


class BarcodeScanner:
    """Scanner for extracting barcodes from images"""
    
    def __init__(self):
        # Supported barcode formats for food products
        self.supported_formats = {
            "EAN13": 13,    # Most common for food products
            "EAN8": 8,     # Smaller food products
            "UPCA": 12,     # US/Canada products
            "UPCE": 9,     # Short UPC
            "CODE128": 128,  # Some food products
        }

    def extract_barcode_from_image(self, image_bytes: bytes) -> Optional[str]:
        """Extract barcode from image bytes with enhanced debugging"""
        try:
            # Convert bytes to PIL Image
            image = Image.open(BytesIO(image_bytes))
        
            # Convert to RGB if necessary
            if image.mode != 'RGB':
                image = image.convert('RGB')
        
            # Decode barcodes
            barcodes = pyzbar.decode(image)
        
            if not barcodes:
                logger.info("No barcodes detected in image")
                return None
        
            logger.info(f"Detected {len(barcodes)} barcode(s) in image")
        
            # DEBUG: Show all detected barcodes
            for i, code in enumerate(barcodes):
                logger.info(f"Barcode {i+1}: type={code.type}, data='{code.data.decode('utf-8')}', format={code.type}")
        
            # Find the first valid barcode
            for barcode in barcodes:
                barcode_data = barcode.data.decode('utf-8')
                # DEBUG: Show all detected barcodes
                logger.info(f"Checking barcode: '{barcode_data}' (type: {barcode.type})")
                logger.info(f"barcode.type = {barcode.type}")
                logger.info(f"type of barcode.type = {type(barcode.type)}")
                logger.info(f"pyzbar.ZBarSymbol.EAN13 = {pyzbar.ZBarSymbol.EAN13}")
                logger.info(f"Are they equal? {barcode.type == pyzbar.ZBarSymbol.EAN13}")
                logger.info(f"Is barcode.type in supported_formats? {barcode.type in self.supported_formats}")            
                # CHECK 1: Supported format ?
                if barcode.type not in self.supported_formats:
                    logger.warning(f"Unsupported format: {barcode.type} (supported: {self.supported_formats})")
                    continue
            
                # CHECK 2: Valid format ?
                if self._is_valid_barcode(barcode_data):
                    logger.info(f"Valid barcode found: {barcode_data} (type: {barcode.type})")
                    return barcode_data
                else:
                    logger.warning(f"Invalid barcode format: '{barcode_data}' (length: {len(barcode_data)})")
        
            logger.info("No valid barcodes found in supported formats")
            return None
        
        except Exception as e:
            logger.error(f"Error processing image: {e}")
            return None        
    
    def extract_barcode_from_image_original(self, image_bytes: bytes) -> Optional[str]:
        """
        Extract barcode from image bytes
        
        Args:
            image_bytes: Image data as bytes
            
        Returns:
            Barcode string if found, None otherwise
        """
        try:
            # Convert bytes to PIL Image
            image = Image.open(BytesIO(image_bytes))
            
            # Convert to RGB if necessary (some images are RGBA or other formats)
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Decode barcodes
            barcodes = pyzbar.decode(image)
            
            if not barcodes:
                logger.info("No barcodes detected in image")
                return None
            
            # Find the first valid barcode
            for barcode in barcodes:
                if barcode.type in self.supported_formats:
                    barcode_data = barcode.data.decode('utf-8')
                    
                    # Validate barcode format (8-18 digits as per database constraint)
                    if self._is_valid_barcode(barcode_data):
                        logger.info(f"Valid barcode found: {barcode_data} (type: {barcode.type})")
                        return barcode_data
                    else:
                        logger.warning(f"Invalid barcode format: {barcode_data}")
            
            logger.info("No valid barcodes found in supported formats")
            return None
            
        except Exception as e:
            logger.error(f"Error processing image: {e}")
            return None
    
    def _is_valid_barcode(self, barcode: str) -> bool:
        """
        Validate barcode format according to database constraints
        Uses constants from configuration
        
        Args:
            barcode: Barcode string to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check if it's all digits and correct length (using constants)
        if not barcode.isdigit():
            return False
        
        min_length = DATA_QUALITY_THRESHOLDS["min_barcode_length"]
        max_length = DATA_QUALITY_THRESHOLDS["max_barcode_length"]
        
        if len(barcode) < min_length or len(barcode) > max_length:
            return False
        
        return True
    
    def enhance_image_for_scanning(self, image: Image.Image) -> Image.Image:
        """
        Enhance image quality for better barcode detection
        Can be used for difficult-to-read barcodes
        
        Args:
            image: PIL Image object
            
        Returns:
            Enhanced PIL Image
        """
        try:
            # Convert to grayscale for better contrast
            if image.mode != 'L':
                image = image.convert('L')
            
            # Increase contrast and sharpness
            from PIL import ImageEnhance
            
            # Enhance contrast
            enhancer = ImageEnhance.Contrast(image)
            image = enhancer.enhance(2.0)
            
            # Enhance sharpness
            enhancer = ImageEnhance.Sharpness(image)
            image = enhancer.enhance(2.0)
            
            return image
            
        except Exception as e:
            logger.error(f"Error enhancing image: {e}")
            return image
    
    def extract_barcode_with_enhancement(self, image_bytes: bytes) -> Optional[str]:
        """
        Extract barcode with image enhancement fallback
        Tries normal extraction first, then enhanced version
        
        Args:
            image_bytes: Image data as bytes
            
        Returns:
            Barcode string if found, None otherwise
        """
        # Try normal extraction first
        barcode = self.extract_barcode_from_image(image_bytes)
        if barcode:
            return barcode
        
        # Try with image enhancement
        try:
            image = Image.open(BytesIO(image_bytes))
            enhanced_image = self.enhance_image_for_scanning(image)
            
            # Convert back to bytes
            buffer = BytesIO()
            enhanced_image.save(buffer, format='PNG')
            enhanced_bytes = buffer.getvalue()
            
            # Try extraction again
            barcode = self.extract_barcode_from_image(enhanced_bytes)
            if barcode:
                logger.info(f"Barcode found with enhancement: {barcode}")
                return barcode
            
        except Exception as e:
            logger.error(f"Error in enhanced extraction: {e}")
        
        return None
    
    def get_scanner_info(self) -> dict:
        """
        Get information about scanner capabilities
        Useful for debugging and monitoring
        
        Returns:
            Dict with scanner information
        """
        return {
            "supported_formats": [str(fmt) for fmt in self.supported_formats],
            "pyzbar_version": pyzbar.__version__ if hasattr(pyzbar, '__version__') else "unknown",
            "enhancement_available": True
        }