import re

def add_markdown_to_docx(doc, md_text):
    md_text = _preprocess_markdown(md_text)
    lines = md_text.split("\n")

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        stripped = line.strip()

        if not stripped:
            i += 1
            continue

        # headings
        if stripped.startswith("# "):
            doc.add_heading(stripped[2:].strip(), level=1)
            i += 1
            continue
        if stripped.startswith("## "):
            doc.add_heading(stripped[3:].strip(), level=2)
            i += 1
            continue
        if stripped.startswith("### "):
            doc.add_heading(stripped[4:].strip(), level=3)
            i += 1
            continue
        if stripped.startswith("#### "):
            doc.add_heading(stripped[5:].strip(), level=4)
            i += 1
            continue

        # table block: at least 2 consecutive pipe-lines
        if _looks_like_table_line(stripped):
            table_lines = [stripped]
            j = i + 1
            while j < len(lines) and _looks_like_table_line(lines[j].strip()):
                table_lines.append(lines[j].strip())
                j += 1

            _render_table(doc, table_lines)
            i = j
            continue

        # bulleted list
        if stripped.startswith("- ") or stripped.startswith("* ") or stripped.startswith("+ "):
            p = doc.add_paragraph(style="List Bullet")
            _parse_inline_formatting(p, stripped[2:].strip())
            i += 1
            continue

        # numbered list
        if re.match(r'^\d+[\.\)]\s+', stripped):
            p = doc.add_paragraph(style="List Number")
            clean_line = re.sub(r'^\d+[\.\)]\s+', '', stripped).strip()
            _parse_inline_formatting(p, clean_line)
            i += 1
            continue

        # normal paragraph
        p = doc.add_paragraph()
        _parse_inline_formatting(p, stripped)
        i += 1


def _preprocess_markdown(text: str) -> str:
    text = (text or "").replace("\r\n", "\n")

    # force headings onto new lines
    text = re.sub(r'(?<!\n)(##\s+\d)', r'\n\1', text)
    text = re.sub(r'(?<!\n)(##\s+0\.)', r'\n\1', text)
    text = re.sub(r'(?<!\n)(#\s+Юридическое заключение по тендеру)', r'\n\1', text)

    # split heading glued to table header
    fixed_lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith("## ") and "|" in stripped:
            left, right = stripped.split("|", 1)
            fixed_lines.append(left.strip())
            fixed_lines.append("|" + right)
        else:
            fixed_lines.append(line)

    text = "\n".join(fixed_lines)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _looks_like_table_line(line: str) -> bool:
    if "|" not in line:
        return False
    return line.count("|") >= 2


def _normalize_table_lines(table_lines):
    cleaned = []
    for line in table_lines:
        line = line.strip()
        if not line:
            continue
        if not line.startswith("|"):
            line = "| " + line.lstrip("| ").rstrip() + " |"
        if not line.endswith("|"):
            line = line.rstrip(" |") + " |"

        parts = [p.strip() for p in line.strip("|").split("|")]
        if not any(parts):
            continue
        cleaned.append(parts)

    if not cleaned:
        return []

    # if second line is separator-like, drop it from content
    if len(cleaned) >= 2:
        joined = "|".join(cleaned[1])
        if re.fullmatch(r'[\s:\-]+(?:\|[\s:\-]+)*', joined):
            cleaned.pop(1)

    return cleaned


def _render_table(doc, table_lines):
    rows = _normalize_table_lines(table_lines)
    if not rows:
        return

    num_cols = max(len(r) for r in rows)
    if num_cols == 0:
        return

    table = doc.add_table(rows=len(rows), cols=num_cols)
    table.style = "Table Grid"

    for i, row in enumerate(rows):
        padded = row + [""] * (num_cols - len(row))
        for j, col_text in enumerate(padded):
            cell = table.cell(i, j)
            cell.text = ""
            p = cell.paragraphs[0]
            _parse_inline_formatting(p, col_text)
            if i == 0:
                for run in p.runs:
                    run.bold = True


def _parse_inline_formatting(paragraph, text):
    pattern = r'(\*\*\*.*?\*\*\*|___.*?___|\*\*.*?\*\*|__.*?__|(?<!\*)\*.*?\*(?!\*)|\b_.*?_\b)'
    tokens = re.split(pattern, text)

    for token in tokens:
        if not token:
            continue

        if (token.startswith('***') and token.endswith('***')) or (token.startswith('___') and token.endswith('___')):
            run = paragraph.add_run(token[3:-3])
            run.bold = True
            run.italic = True
        elif (token.startswith('**') and token.endswith('**')) or (token.startswith('__') and token.endswith('__')):
            run = paragraph.add_run(token[2:-2])
            run.bold = True
        elif (token.startswith('*') and token.endswith('*')) or (token.startswith('_') and token.endswith('_')):
            run = paragraph.add_run(token[1:-1])
            run.italic = True
        else:
            paragraph.add_run(token)
