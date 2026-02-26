# derivations.py

class DerivationRegistry:
    """
    Registry for deriving new values.
    """

    @staticmethod
    def extract_email_domain(value: str) -> str:
        """Extracts the domain (after @) from an email."""
        try:
            return str(value).split('@')[1]
        except:
            return None

    @staticmethod
    def combine_fields(value: str, append_text: str) -> str:
        """Appends text to the current value."""
        return f"{value} {append_text}"

    @staticmethod
    def calculate_tax(value: float, tax_percentage: float) -> float:
        """Calculates tax based on percentage."""
        try:
            return float(value) * (float(tax_percentage) / 100.0)
        except:
            return 0.0