import os

# Configuration: What to ignore so the file doesn't get cluttered with junk or secrets
IGNORE_DIRS = {'.git', '.streamlit', '__pycache__', 'env', 'venv', 'api5', 'api6', 'api7', 'data'}
IGNORE_FILES = {'Master_Code.txt', 'Master.py', 'secrets.toml'} 
ALLOWED_EXTENSIONS = {'.py', '.txt', '.md'} # We only extract actual code/info files

def generate_tree(directory, prefix=""):
    """Maps the physical folder structure."""
    tree_str = ""
    try:
        items = os.listdir(directory)
    except PermissionError:
        return ""

    # Filter and sort
    items = [i for i in items if i not in IGNORE_DIRS]
    items.sort()
    
    for i, item in enumerate(items):
        path = os.path.join(directory, item)
        is_last = (i == len(items) - 1)
        pointer = "└── " if is_last else "├── "
        tree_str += prefix + pointer + item + "\n"
        
        if os.path.isdir(path):
            extension = "    " if is_last else "│   "
            tree_str += generate_tree(path, prefix=prefix + extension)
            
    return tree_str

def extract_system_snapshot():
    """Compiles the tree and all source code into Master_Code.txt"""
    output_filename = "Master_Code.txt"
    
    print("Initiating Multiversal Code Extraction...")
    
    with open(output_filename, 'w', encoding='utf-8') as outfile:
        # 1. Write the Header
        outfile.write("🌌 MULTIVERSAL CODE MASTER - SYSTEM SNAPSHOT 🌌\n")
        outfile.write("="*70 + "\n")
        
        # 2. Write the Tree Structure
        outfile.write("📂 REPOSITORY TREE STRUCTURE:\n\n")
        outfile.write("Nepse_Data/\n")
        tree_output = generate_tree(".")
        outfile.write(tree_output)
        outfile.write("\n" + "="*70 + "\n\n")
        
        # 3. Write the Code Files
        outfile.write("📜 SYSTEM SOURCE CODES:\n\n")
        
        total_files = 0
        total_lines = 0
        
        for root, dirs, files in os.walk("."):
            # Exclude ignored directories from the walk
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            
            for file in files:
                if file in IGNORE_FILES:
                    continue
                    
                if any(file.endswith(ext) for ext in ALLOWED_EXTENSIONS):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            content = infile.read()
                            lines = content.count('\n') + 1
                            
                            total_files += 1
                            total_lines += lines
                            
                            # Format the file output clearly
                            outfile.write(f"/// START OF FILE: {file_path} ///\n")
                            outfile.write(content)
                            outfile.write("\n/// END OF FILE ///\n\n")
                            outfile.write("-" * 70 + "\n\n")
                            
                    except Exception as e:
                        outfile.write(f"/// ERROR READING FILE: {file_path} | {e} ///\n\n")
                        
        # 4. Write Metrics Summary
        outfile.write("="*70 + "\n")
        outfile.write("📊 SNAPSHOT METRICS:\n")
        outfile.write(f"Total Files Extracted: {total_files}\n")
        outfile.write(f"Total Lines of Code:   {total_lines}\n")
        outfile.write("="*70 + "\n")
        
    print(f"✅ Extraction Complete! {total_files} files written to '{output_filename}'.")
    print(f"Total Lines of Code: {total_lines}")

if __name__ == "__main__":
    extract_system_snapshot()
