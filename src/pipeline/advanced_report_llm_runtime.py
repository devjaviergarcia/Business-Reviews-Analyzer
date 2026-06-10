from __future__ import annotations

import re


def extract_json_object(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("Empty LLM response.")
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    inline = re.search(r"(\{.*\})", raw, re.DOTALL)
    if inline:
        return inline.group(1).strip()
    raise ValueError("Could not extract JSON object from LLM response.")


def llm_generate_text(
    *,
    prompt: str,
    client,
    primary_model_name: str,
    fallback_models: list[str],
    genai_errors_module,
    extract_llm_text,
) -> tuple[str, str | None]:
    candidates = list(dict.fromkeys([primary_model_name, *fallback_models]))
    last_error: Exception | None = None
    for model_name in candidates:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
            )
            text = extract_llm_text(response)
            if text:
                return text, model_name
        except Exception as exc:
            if genai_errors_module is not None and isinstance(
                exc,
                getattr(genai_errors_module, "ClientError", Exception),
            ):
                code = getattr(exc, "code", None)
                if code == 404:
                    last_error = exc
                    continue
            last_error = exc
            continue
    if last_error:
        raise last_error
    return "", None


def extract_llm_text(response: object) -> str:
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
    return "\n".join(item for item in texts if item).strip()


def sanitize_llm_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    output = value
    output = output.replace("**", "")
    output = re.sub(r"\bimpactoo\b", "impacto", output, flags=re.IGNORECASE)
    output = re.sub(r"([A-Za-zÁÉÍÓÚÜÑáéíóúüñ])\1{3,}", r"\1\1", output)
    output = re.sub(r"([aeiouáéíóúüAEIOUÁÉÍÓÚÜ])\1{2,}", r"\1", output)
    output = re.sub(
        r"\b([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{4,})([aeiouáéíóúüAEIOUÁÉÍÓÚÜ])\2\b",
        r"\1\2",
        output,
    )
    output = re.sub(r"\.{2,}", ".", output)
    output = re.sub(r"[ \t]{2,}", " ", output)
    output = re.sub(r"\s+\n", "\n", output)
    output = re.sub(r"\n{3,}", "\n\n", output)
    output = re.sub(r"([a-záéíóúüñ])([A-ZÁÉÍÓÚÜÑ])", r"\1 \2", output)
    return output.strip()
