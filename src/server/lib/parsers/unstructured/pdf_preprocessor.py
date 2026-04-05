"""PDF log preprocessor — extract text from scanned PDF logs using vision LLM.

Integrates with the unstructured pipeline to handle ``.pdf`` files by
converting each page to an image and extracting text via a vision-capable
LLM (Gemini Vision or similar via OpenRouter).

Usage within the pipeline::

    from lib.parsers.unstructured.pdf_preprocessor import PDFPreprocessor

    preprocessor = PDFPreprocessor()
    if preprocessor.can_handle(filename):
        text = preprocessor.extract(pdf_bytes)
        # Feed text into the normal unstructured pipeline
"""

from __future__ import annotations

import base64
import logging
from typing import TYPE_CHECKING

from lib import ai

logger = logging.getLogger(__name__)

# Vision model for OCR — defaults to Gemini Flash Lite for cost efficiency.
_VISION_MODEL = ai.SEMICONDUCTOR_LLM_MODEL

# Supported PDF extensions.
_PDF_EXTENSIONS = frozenset({".pdf"})


class PDFPreprocessor:
    """Extract text from scanned PDF logs using a vision-capable LLM.

    Requires ``pymupdf`` (fitz) for PDF-to-image conversion.  This is an
    optional dependency — the preprocessor gracefully degrades if it is
    not installed.
    """

    def __init__(self, vision_model: str | None = None) -> None:
        self.vision_model = vision_model or _VISION_MODEL

    @staticmethod
    def can_handle(filename: str) -> bool:
        """Return True if the filename looks like a PDF."""
        return any(filename.lower().endswith(ext) for ext in _PDF_EXTENSIONS)

    @staticmethod
    def is_available() -> bool:
        """Return True if pymupdf is installed."""
        try:
            import fitz  # noqa: F401

            return True
        except ImportError:
            return False

    def extract(self, pdf_bytes: bytes) -> str:
        """Convert PDF to text via page-by-page OCR.

        Returns the concatenated text from all pages.

        Raises
        ------
        ImportError
            If ``pymupdf`` is not installed.
        RuntimeError
            If the LLM API key is not configured.
        """
        if not self.is_available():
            raise ImportError(
                "pymupdf is required for PDF extraction. Install with: uv add pymupdf"
            )

        if not ai.has_openrouter_api_key():
            raise RuntimeError(
                "OPENROUTER_API_KEY not set; cannot perform PDF OCR."
            )

        images = self._pdf_to_images(pdf_bytes)
        logger.info("PDFPreprocessor: converting %d page(s) to text", len(images))

        pages: list[str] = []
        for i, img_b64 in enumerate(images, 1):
            text = self._ocr_page(img_b64, i)
            pages.append(text)

        return "\n".join(pages)

    @staticmethod
    def _pdf_to_images(pdf_bytes: bytes) -> list[str]:
        """Convert PDF pages to base64-encoded PNGs using PyMuPDF."""
        import fitz

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        images: list[str] = []

        for page in doc:
            # Render at 2x resolution for better OCR quality.
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            png_data = pix.tobytes("png")
            images.append(base64.b64encode(png_data).decode("ascii"))

        doc.close()
        return images

    def _ocr_page(self, image_b64: str, page_num: int) -> str:
        """Send a single page image to the vision LLM for text extraction."""
        from langchain_openrouter import ChatOpenRouter
        from pydantic import SecretStr

        model = ChatOpenRouter(
            model=self.vision_model,
            api_key=SecretStr(ai.resolve_openrouter_api_key()),
            temperature=0.0,
            max_tokens=4096,
        )

        messages = [
            (
                "system",
                "You are an OCR assistant specializing in semiconductor equipment logs. "
                "Extract ALL text from the provided image exactly as it appears, preserving "
                "line breaks, spacing, and formatting. Do not add any commentary — output "
                "only the raw extracted text.",
            ),
            (
                "human",
                [
                    {"type": "text", "text": f"Extract all text from page {page_num}:"},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{image_b64}"},
                    },
                ],
            ),
        ]

        try:
            response = model.invoke(messages)
            return str(response.content)
        except Exception as exc:
            logger.warning("PDFPreprocessor: OCR failed for page %d: %s", page_num, exc)
            return f"[OCR FAILED: page {page_num}]"
