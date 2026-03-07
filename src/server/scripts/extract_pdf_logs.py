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
from langchain_openrouter import ChatOpenRouter
from pydantic import SecretStr

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
# Use a vision-capable model for OCR.
VISION_MODEL = os.getenv("OPENROUTER_VISION_MODEL", "anthropic/claude-sonnet-4")


def extract_text_from_page(page_image_b64: str, page_num: int) -> str:
    """Send a single page image to the LLM for text extraction."""
    if not OPENROUTER_API_KEY:
        print("Error: OPENROUTER_API_KEY not set in environment.", file=sys.stderr)
        sys.exit(1)

    model = ChatOpenRouter(
        model=VISION_MODEL,
        api_key=SecretStr(OPENROUTER_API_KEY),
        temperature=0.0,
        max_tokens=4096,
    )

    messages = [
        (
            "system",
            "You are an OCR assistant. Extract ALL text from the provided image exactly "
            "as it appears, preserving line breaks, spacing, and formatting. Do not add "
            "any commentary — output only the raw extracted text.",
        ),
        (
            "human",
            [
                {"type": "text", "text": f"Extract all text from page {page_num}:"},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{page_image_b64}"},
                },
            ],
        ),
    ]

    response = model.invoke(messages)
    return str(response.content)


def pdf_to_images(pdf_path: Path) -> list[str]:
    """Convert each PDF page to a base64-encoded PNG.

    Requires the ``pymupdf`` (fitz) package.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        print(
            "Error: pymupdf is required for PDF extraction.\n"
            "  Install with: uv add pymupdf",
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
