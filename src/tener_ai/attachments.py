from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence


NAME_KEYS: Sequence[str] = ("name", "filename", "file_name", "title")
URL_KEYS: Sequence[str] = (
    "url",
    "link",
    "href",
    "download_url",
    "downloadUrl",
    "signed_url",
    "signedUrl",
    "public_url",
    "publicUrl",
    "file_url",
    "fileUrl",
)
MIME_KEYS: Sequence[str] = ("mime_type", "mimeType", "content_type", "contentType")
SIZE_KEYS: Sequence[str] = ("size", "size_bytes", "sizeBytes", "content_length", "contentLength")
ID_KEYS: Sequence[str] = ("id", "file_id", "fileId", "document_id", "documentId", "asset_id", "assetId")

RESUME_MARKERS: Sequence[str] = (
    "resume",
    "cv",
    "curriculum",
    "currículum",
    ".pdf",
    ".doc",
    ".docx",
)


@dataclass
class AttachmentDescriptor:
    name: Optional[str]
    url: Optional[str]
    mime_type: Optional[str]
    size_bytes: Optional[int]
    provider_file_id: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "url": self.url,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "provider_file_id": self.provider_file_id,
        }


def extract_attachment_descriptors_from_values(values: Sequence[Any], limit: int = 12) -> List[AttachmentDescriptor]:
    out: List[AttachmentDescriptor] = []
    seen: set[str] = set()
    safe_limit = max(1, min(int(limit or 12), 64))
    for value in values:
        _collect_descriptors(value=value, out=out, seen=seen, limit=safe_limit)
        if len(out) >= safe_limit:
            break
    return out[:safe_limit]


def descriptors_to_text(descriptors: Sequence[AttachmentDescriptor], limit: int = 12) -> str:
    fragments: List[str] = []
    seen: set[str] = set()
    safe_limit = max(1, min(int(limit or 12), 64))
    for item in descriptors[:safe_limit]:
        name = str(item.name or "").strip()
        url = str(item.url or "").strip()
        if url:
            text = f"attached file {name} {url}".strip() if name else f"attached file {url}"
        elif name:
            text = f"attached file {name}"
        else:
            continue
        token = text.lower()
        if token in seen:
            continue
        seen.add(token)
        fragments.append(text)
        if len(fragments) >= safe_limit:
            break
    return "\n".join(fragments).strip()


def is_resume_like_name_or_url(value: str) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    return any(marker in lowered for marker in RESUME_MARKERS)


def extract_resume_urls(descriptors: Sequence[AttachmentDescriptor]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in descriptors:
        url = str(item.url or "").strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            continue
        name = str(item.name or "").strip()
        mime_type = str(item.mime_type or "").strip().lower()
        if not (is_resume_like_name_or_url(url) or is_resume_like_name_or_url(name) or "pdf" in mime_type):
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _collect_descriptors(value: Any, out: List[AttachmentDescriptor], seen: set[str], limit: int) -> None:
    if len(out) >= limit:
        return
    if isinstance(value, dict):
        name = _pick_str(value, NAME_KEYS)
        url = _pick_url(value, URL_KEYS)
        mime_type = _pick_str(value, MIME_KEYS)
        provider_file_id = _pick_str(value, ID_KEYS)
        size_bytes = _pick_int(value, SIZE_KEYS)

        if _looks_like_attachment(name=name, url=url, mime_type=mime_type):
            key = _descriptor_key(name=name, url=url, provider_file_id=provider_file_id)
            if key and key not in seen:
                seen.add(key)
                out.append(
                    AttachmentDescriptor(
                        name=name,
                        url=url,
                        mime_type=mime_type,
                        size_bytes=size_bytes,
                        provider_file_id=provider_file_id,
                    )
                )
                if len(out) >= limit:
                    return

        for nested in value.values():
            _collect_descriptors(value=nested, out=out, seen=seen, limit=limit)
            if len(out) >= limit:
                return
        return

    if isinstance(value, list):
        for item in value:
            _collect_descriptors(value=item, out=out, seen=seen, limit=limit)
            if len(out) >= limit:
                return


def _pick_str(payload: Dict[str, Any], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        raw = payload.get(key)
        if isinstance(raw, str):
            cleaned = raw.strip()
            if cleaned:
                return cleaned
    return None


def _pick_int(payload: Dict[str, Any], keys: Sequence[str]) -> Optional[int]:
    for key in keys:
        raw = payload.get(key)
        try:
            if raw is None:
                continue
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value >= 0:
            return value
    return None


def _pick_url(payload: Dict[str, Any], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        raw = payload.get(key)
        if isinstance(raw, str):
            cleaned = raw.strip()
            if cleaned.startswith("http://") or cleaned.startswith("https://"):
                return cleaned
    return None


def _looks_like_attachment(name: Optional[str], url: Optional[str], mime_type: Optional[str]) -> bool:
    if url:
        return True
    if name:
        lowered = name.lower()
        if "." in lowered:
            return True
        if any(marker in lowered for marker in ("cv", "resume", "document", "attachment", "file")):
            return True
    if mime_type and "/" in mime_type:
        return True
    return False


def _descriptor_key(name: Optional[str], url: Optional[str], provider_file_id: Optional[str]) -> str:
    if url:
        return f"url:{url.strip().lower()}"
    if provider_file_id:
        return f"id:{provider_file_id.strip().lower()}"
    if name:
        return f"name:{name.strip().lower()}"
    return ""
