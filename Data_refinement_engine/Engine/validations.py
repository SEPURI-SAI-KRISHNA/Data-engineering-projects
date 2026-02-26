# validations.py
import re

class ValidationRegistry:
    """
    Registry for validation logic. Returns Boolean (True/False).
    """

    @staticmethod
    def not_null(value: any) -> bool:
        """Checks if value is not empty or None."""
        return value is not None and str(value).strip() != ""

    @staticmethod
    def is_valid_email(value: str) -> bool:
        """Checks if the string matches email format."""
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        return bool(re.match(pattern, str(value)))

    @staticmethod
    def min_length(value: str, min_chars: int) -> bool:
        """Checks if string meets minimum length."""
        return len(str(value)) >= int(min_chars)

    @staticmethod
    def number_range(value: float, min_value: float, max_value: float) -> bool:
        """Checks if number is within range."""
        try:
            val = float(value)
            return float(min_value) <= val <= float(max_value)
        except:
            return False

    @staticmethod
    def validate_name_length(value: str, min_length: int = 1, max_length: int = 50) -> bool:
        """Check if the provided name is within the valid length range."""
        return min_length <= len(value) <= max_length
