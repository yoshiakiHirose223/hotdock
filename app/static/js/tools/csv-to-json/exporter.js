let activeBlobUrl = "";

export function createJsonDownloadUrl(jsonText) {
  revokeDownloadUrl();
  const blob = new Blob([jsonText], { type: "application/json;charset=utf-8" });
  activeBlobUrl = URL.createObjectURL(blob);
  return activeBlobUrl;
}

export function revokeDownloadUrl() {
  if (activeBlobUrl) {
    URL.revokeObjectURL(activeBlobUrl);
    activeBlobUrl = "";
  }
}
