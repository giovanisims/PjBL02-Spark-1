def format_item(item):
    if isinstance(item, float):
        return f"{item:,.2f}"
    if isinstance(item, int):
        return f"{item:,}"
    if isinstance(item, tuple):
        return "(" + ", ".join(format_item(x) for x in item) + ")"
    if isinstance(item, list):
        return "[" + ", ".join(format_item(x) for x in item) + "]"
    return str(item)