import os, re
with open("mt5_extracao/ui_manager.py", "r", encoding="utf-8") as f:
    content = f.read()
pattern1 = r"(                        else:\n                            messagebox\.showwarning\(\"Aviso\", \"Nenhum dado encontrado para exportar\"\)\n)                        messagebox\.showerror"
replacement1 = r"\1                    except Exception as e:\n                        messagebox.showerror"
fixed = re.sub(pattern1, replacement1, content)
with open("mt5_extracao/ui_manager.py", "w", encoding="utf-8") as f:
    f.write(fixed)
print("Arquivo corrigido")
