function isAbsoluteFilesystemPath(value: string): boolean {
  return value.startsWith("/") || /^[A-Za-z]:[\\/]/.test(value);
}

function toFileUrl(pathValue: string): string {
  const normalizedPath = pathValue.replace(/\\/g, "/");
  const withLeadingSlash = normalizedPath.startsWith("/") ? normalizedPath : `/${normalizedPath}`;
  return `file://${encodeURI(withLeadingSlash)}`;
}

export function extractLocalArtifactPath(pathOrUrl: string): string | null {
  const value = String(pathOrUrl || "").trim();
  if (!value) return null;
  if (value.startsWith("file://")) {
    const decoded = decodeURI(value.slice("file://".length));
    return decoded || null;
  }
  if (isAbsoluteFilesystemPath(value)) {
    return value;
  }
  return null;
}

export function normalizeArtifactOutputUrl(pathOrUrl: string, apiBaseUrl: string): string {
  const value = String(pathOrUrl || "").trim();
  if (!value) return "";
  if (value.startsWith("http://") || value.startsWith("https://") || value.startsWith("blob:")) {
    return value;
  }
  if (value.startsWith("file://")) {
    return value;
  }
  if (isAbsoluteFilesystemPath(value)) {
    return toFileUrl(value);
  }

  const normalizedBase = String(apiBaseUrl || "").trim().replace(/\/+$/, "");
  if (value.startsWith("/business/report/artifacts")) {
    return `${normalizedBase}${value}`;
  }
  return `${normalizedBase}/business/report/artifacts?path=${encodeURIComponent(value)}`;
}
