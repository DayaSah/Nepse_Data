import os

def print_tree(directory, prefix=""):
    # Ignore these folders to keep the tree clean
    ignore_dirs = {'.git', '.streamlit', '__pycache__', 'env', 'venv'}
    
    try:
        items = os.listdir(directory)
    except PermissionError:
        return

    items = [i for i in items if i not in ignore_dirs]
    items.sort()
    
    for i, item in enumerate(items):
        path = os.path.join(directory, item)
        is_last = (i == len(items) - 1)
        pointer = "└── " if is_last else "├── "
        print(prefix + pointer + item)
        
        if os.path.isdir(path):
            extension = "    " if is_last else "│   "
            print_tree(path, prefix=prefix + extension)

def get_metrics(directory):
    total_lines = 0
    py_files = 0
    for root, dirs, files in os.walk(directory):
        if any(ignore in root for ignore in ['.git', '__pycache__', 'env', 'venv']):
            continue
        for file in files:
            if file.endswith('.py'):
                py_files += 1
                try:
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        total_lines += len(f.readlines())
                except:
                    pass
    return py_files, total_lines

if __name__ == "__main__":
    print("\n🌌 MULTIVERSAL CODE MASTER INITIATED 🌌")
    print("="*45)
    print("📂 REPOSITORY TREE STRUCTURE:")
    print("Nepse_Data/")
    print_tree(".")
    
    print("\n📊 QUANTUM METRICS:")
    py_files, lines = get_metrics(".")
    print(f"Total Python Nodes (.py files): {py_files}")
    print(f"Total Lines of Logic: {lines}")
    print("="*45)
    print("Status: System Architecture Mapped Successfully.\n")
