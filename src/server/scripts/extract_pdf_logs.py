#!/usr/bin/env python3
"""Extract text from PDF log files using a vision-capable LLM.

Usage:
    python scripts/extract_pdf_logs.py input.pdf [-o output.log]

Sends each page as a base64 image to the LLM and concatenates the
extracted text into a single plain-text log file suitable for the
unstructured parser.
"""

import argparse
import base64
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

SERVER_SRC = Path(__file__).resolve().parents[1] / "src"
if str(SERVER_SRC) not in sys.path:
    sys.path.insert(0, str(SERVER_SRC))

from lib import ai

load_dotenv()

VISION_MODEL = ai.resolve_openrouter_vision_model()


def extract_text_from_page(page_image_b64: str, page_num: int) -> str:
    """Send a single page image to the LLM for text extraction."""
    if not ai.has_openrouter_api_key():
        print("Error: OPENROUTER_API_KEY not set in environment.", file=sys.stderr)
        sys.exit(1)

    invocation = ai.extract_text_from_image(
        page_image_b64=page_image_b64,
        page_num=page_num,
        model=VISION_MODEL,
    )
    if invocation.response is None:
        warning = invocation.warning or "Unknown OCR failure."
        print(f"Error: {warning}", file=sys.stderr)
        sys.exit(1)

    return invocation.response


def pdf_to_images(pdf_path: Path) -> list[str]:
    """Convert each PDF page to a base64-encoded PNG.

    Requires the ``pymupdf`` (fitz) package.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        print(
            "Error: pymupdf is required for PDF extraction.\n  Install with: uv add pymupdf",
            file=sys.stderr,
        )
        sys.exit(1)

    doc = fitz.open(str(pdf_path))
    images: list[str] = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        # Render at 2x for better OCR quality.
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        png_data = pix.tobytes("png")
        images.append(base64.b64encode(png_data).decode("ascii"))

    doc.close()
    return images


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract text from PDF log files via vision LLM.")
    parser.add_argument("pdf", type=Path, help="Path to the PDF file.")
    parser.add_argument("-o", "--output", type=Path, help="Output text file (default: <input>.log).")
    args = parser.parse_args()

    if not args.pdf.exists():
        print(f"Error: {args.pdf} not found.", file=sys.stderr)
        sys.exit(1)

    output_path = args.output or args.pdf.with_suffix(".log")

    print(f"Converting {args.pdf} to images...")
    page_images = pdf_to_images(args.pdf)
    print(f"Found {len(page_images)} page(s).")

    all_text: list[str] = []
    for i, img_b64 in enumerate(page_images, 1):
        print(f"  Extracting page {i}/{len(page_images)}...")
        text = extract_text_from_page(img_b64, i)
        all_text.append(text)

    combined = "\n".join(all_text)
    output_path.write_text(combined, encoding="utf-8")
    print(f"Wrote {len(combined)} chars to {output_path}")


if __name__ == "__main__":
    main()
