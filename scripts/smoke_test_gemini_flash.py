import argparse
import sys
from pathlib import Path

from google import genai
from google.genai import errors as genai_errors

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test for Gemini model connectivity.")
    parser.add_argument(
        "--model",
        default="gemini-1.5-flash",
        help="Gemini model to call (default: gemini-1.5-flash).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is empty. Set it in .env first.")

    client = genai.Client(api_key=settings.gemini_api_key)
    candidates = list(dict.fromkeys([args.model, "gemini-flash-latest", "gemini-2.5-flash"]))
    response = None
    used_model = None

    for model_name in candidates:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents="Reply exactly with: GEMINI_OK",
            )
            used_model = model_name
            break
        except genai_errors.ClientError as exc:
            if exc.code == 404:
                continue
            raise

    if response is None or used_model is None:
        flash_models = [m.name for m in client.models.list() if "flash" in m.name.lower()]
        raise RuntimeError(
            f"No Flash model available for generate_content. Requested: {args.model}. "
            f"Available Flash models: {flash_models[:10]}"
        )

    text = _extract_text(response)
    if not text:
        raise RuntimeError("Gemini returned an empty response.")

    print(f"Requested model: {args.model}")
    print(f"Used model: {used_model}")
    print(f"Response: {text}")
    print("Connection test: OK")


def _extract_text(response: object) -> str:
    texts: list[str] = []
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        if content is None:
            continue
        for part in getattr(content, "parts", None) or []:
            text = getattr(part, "text", None)
            if text:
                texts.append(str(text).strip())
    return "\n".join([text for text in texts if text]).strip()


if __name__ == "__main__":
    main()
