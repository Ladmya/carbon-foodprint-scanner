#!/usr/bin/env python3
"""
tools/generate_readme.py
Simple README Generator
Generates README files for folders by compiling docstrings from Python files
"""

import re
from pathlib import Path


def extract_docstring(file_path):
    """Extract docstring from a Python file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Look for module-level docstring
        docstring_pattern = r'^"""(.*?)"""'
        match = re.search(docstring_pattern, content, re.DOTALL)
        
        if match:
            return match.group(1).strip()
        
        return None
        
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def generate_folder_readme(folder_path):
    """Generate README content for a folder"""
    if not folder_path.exists():
        return f"# {folder_path.name}\n\n*Folder not found*\n"
    
    # Get all Python files in the folder
    python_files = list(folder_path.glob("*.py"))
    
    if not python_files:
        return f"# {folder_path.name}\n\n*No Python files found*\n"
    
    # Get file information
    files_info = []
    for file_path in sorted(python_files):
        docstring = extract_docstring(file_path)
        if docstring:
            files_info.append({
                'name': file_path.name,
                'docstring': docstring,
                'path': str(file_path)
            })
    
    # Generate README content
    readme_content = f"# {folder_path.name}\n\n"
    
    if files_info:
        readme_content += "## 📁 Files\n\n"
        readme_content += "| File | Description |\n"
        readme_content += "|------|-------------|\n"
        
        for file_info in files_info:
            # Clean up docstring for table
            description = file_info['docstring'].replace('\n', ' ').replace('|', '\\|')
            if len(description) > 80:
                description = description[:77] + "..."
            
            readme_content += f"| `{file_info['name']}` | {description} |\n"
        
        readme_content += "\n## 📋 Details\n\n"
        
        for file_info in files_info:
            readme_content += f"### `{file_info['name']}`\n\n"
            readme_content += f"**Path**: `{file_info['path']}`\n\n"
            readme_content += f"**Description**:\n```\n{file_info['docstring']}\n```\n\n"
    else:
        readme_content += "*No documented Python files found*\n"
    
    return readme_content


def main():
    """Main function to generate folder READMEs"""
    project_root = Path(__file__).parent.parent
    
    folders_to_process = [
        "src/food_scanner/bot",
        "src/food_scanner/data",
        "src/food_scanner/infrastructure",
        "src/food_scanner/core",
        "data_engineering/scripts/setup",
        "data_engineering/scripts/collection",
        "data_engineering/scripts/analysis",
        "data_engineering/scripts/loading"
    ]
    
    print("🔧 Generating folder README files...")
    
    for folder_name in folders_to_process:
        folder_path = project_root / folder_name
        
        if folder_path.exists():
            readme_content = generate_folder_readme(folder_path)
            readme_file = folder_path / "README.md"
            
            # Write README file
            with open(readme_file, 'w', encoding='utf-8') as f:
                f.write(readme_content)
            
            print(f"✅ Generated: {readme_file}")
        else:
            print(f"⚠️  Folder not found: {folder_path}")
    
    print("🎉 Folder README generation complete!")


if __name__ == "__main__":
    main() 