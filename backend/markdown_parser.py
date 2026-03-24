import re
from docx import Document
from docx.shared import Pt, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH

def add_markdown_to_docx(doc, md_text):
    lines = md_text.split('\n')
    
    in_table = False
    table_rows = []
    
    def render_table():
        if not table_rows:
            return
        
        # Remove separator row (e.g. |---|---|)
        valid_rows = [row for row in table_rows if not re.match(r'^\|?[\s\-\|:]+\|?$', row)]
        if not valid_rows:
            table_rows.clear()
            return
            
        # Parse columns
        parsed_rows = []
        for row in valid_rows:
            cols = [col.strip() for col in row.strip('|').split('|')]
            parsed_rows.append(cols)
            
        if not parsed_rows:
            table_rows.clear()
            return
            
        num_cols = max(len(row) for row in parsed_rows)
        table = doc.add_table(rows=len(parsed_rows), cols=num_cols)
        table.style = 'Table Grid'
        
        for i, row in enumerate(parsed_rows):
            for j, col_text in enumerate(row):
                if j < num_cols:
                    try:
                        cell = table.cell(i, j)
                        # Clear existing text
                        cell.text = ''
                        p = cell.paragraphs[0]
                        _parse_inline_formatting(p, col_text)
                        if i == 0:
                            # Header row
                            for run in p.runs:
                                run.bold = True
                    except Exception as e:
                        pass
                            
        table_rows.clear()

    for line in lines:
        line = line.strip()
        
        if line.startswith('|'):
            in_table = True
            table_rows.append(line)
            continue
        else:
            if in_table:
                render_table()
                in_table = False
                
        if not line:
            continue
            
        if line.startswith('# '):
            doc.add_heading(line[2:].strip(), level=1)
        elif line.startswith('## '):
            doc.add_heading(line[3:].strip(), level=2)
        elif line.startswith('### '):
            doc.add_heading(line[4:].strip(), level=3)
        elif line.startswith('#### '):
            doc.add_heading(line[5:].strip(), level=4)
        elif line.startswith('- ') or line.startswith('* '):
            p = doc.add_paragraph(style='List Bullet')
            _parse_inline_formatting(p, line[2:].strip())
        elif re.match(r'^\d+\.\s', line):
            p = doc.add_paragraph(style='List Number')
            _parse_inline_formatting(p, re.sub(r'^\d+\.\s', '', line).strip())
        else:
            p = doc.add_paragraph()
            _parse_inline_formatting(p, line)
            
    if in_table:
        render_table()

def _parse_inline_formatting(paragraph, text):
    # Split by bold and italic markers
    # We will use a simple regex approach
    # **bold**, *italic*, _italic_
    
    # regex to find **bold** or *italic* or _italic_
    # We need to tokenize the text
    tokens = re.split(r'(\*\*.*?\*\*|\*.*?\*|_.*?_)', text)
    
    for token in tokens:
        if token.startswith('**') and token.endswith('**'):
            paragraph.add_run(token[2:-2]).bold = True
        elif token.startswith('*') and token.endswith('*'):
            paragraph.add_run(token[1:-1]).italic = True
        elif token.startswith('_') and token.endswith('_'):
            paragraph.add_run(token[1:-1]).italic = True
        else:
            paragraph.add_run(token)
