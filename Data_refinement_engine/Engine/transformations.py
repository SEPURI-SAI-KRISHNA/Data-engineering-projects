# transformations.py
import math


class TransformationRegistry:
    """
    Registry for data transformation functions.
    Each method must include type hints and a docstring.
    """

    @staticmethod
    def uppercase(value: str) -> str:
        """Converts text to uppercase."""
        return str(value).upper()

    @staticmethod
    def lowercase(value: str) -> str:
        """Converts text to lowercase."""
        return str(value).lower()

    @staticmethod
    def trim_whitespace(value: str) -> str:
        """Removes leading and trailing spaces."""
        return str(value).strip()

    @staticmethod
    def replace_text(value: str, old_value: str, new_value: str) -> str:
        """Replaces specific text with new text."""
        return str(value).replace(old_value, new_value)

    @staticmethod
    def substring(value: str, start_index: int, end_index: int) -> str:
        """Extracts a portion of the text."""
        return str(value)[int(start_index):int(end_index)]

    @staticmethod
    def round_number(value: float, decimals: int) -> float:
        """Rounds a number to specified decimals."""
        try:
            return round(float(value), int(decimals))
        except:
            return value

    @staticmethod
    def math_add(value: float, amount_to_add: float) -> float:
        """Adds a specific amount to the value."""
        try:
            return float(value) + float(amount_to_add)
        except:
            return value