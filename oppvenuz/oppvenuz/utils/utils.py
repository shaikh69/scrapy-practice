import re
import unidecode

def slugify(text):
    # Convert to lowercase
    text = text.lower()
    # Remove accents and special characters
    text = unidecode.unidecode(text)
    # Replace any non-alphanumeric character (except hyphens) with spaces
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    # Replace whitespace and hyphens with a single hyphen
    text = re.sub(r'[\s-]+', '-', text)
    # Strip hyphens from the beginning and end
    text = text.strip('-')
    return text