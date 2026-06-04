from formatter import format_item

def log_result(q_num, title, result):
    header = f"--- {q_num}: {title} ---"
    print(f"\n{header}")
    
    if isinstance(result, list):
        # Print a readable version with formatting for console
        printable_items = [format_item(item) for item in result[:10]]
        print(f"Resultado (primeiros 10 itens): {printable_items}")
        content = "\n".join(format_item(item) for item in result)
    else:
        formatted = format_item(result)
        print(f"Resultado: {formatted}")
        content = formatted
        
    with open(f"results/question_{q_num}.txt", "w", encoding="utf-8") as f:
        f.write(f"{header}\n{content}\n")