const adminSearchInput = document.querySelector("[data-admin-search]");
const adminRows = Array.from(document.querySelectorAll("[data-admin-row]"));
const adminEmptyState = document.querySelector("[data-admin-empty]");
const visibilityToggles = Array.from(document.querySelectorAll("[data-visibility-toggle]"));

const syncAdminEmptyState = () => {
  if (!adminEmptyState || !adminSearchInput) {
    return;
  }

  const query = adminSearchInput.value.trim();
  const visibleCount = adminRows.filter((row) => !row.hidden).length;
  adminEmptyState.hidden = query === "" || visibleCount !== 0;
};

if (adminSearchInput && adminRows.length > 0) {
  const filterRows = () => {
    const query = adminSearchInput.value.trim().toLowerCase();

    adminRows.forEach((row) => {
      const searchIndex = (row.dataset.searchIndex || "").toLowerCase();
      row.hidden = query !== "" && !searchIndex.includes(query);
    });

    syncAdminEmptyState();
  };

  adminSearchInput.addEventListener("input", filterRows);
  filterRows();
}

visibilityToggles.forEach((toggle) => {
  toggle.addEventListener("change", async () => {
    const nextState = toggle.checked;
    const visibilityUrl = toggle.dataset.visibilityUrl;

    if (!visibilityUrl) {
      return;
    }

    toggle.disabled = true;

    try {
      const formData = new FormData();
      formData.append("is_published", nextState ? "true" : "false");

      const response = await fetch(visibilityUrl, {
        method: "POST",
        body: formData,
      });
      const payload = await response.json();

      if (!response.ok) {
        throw new Error((payload.errors || []).join("\n") || "公開状態の更新に失敗しました。");
      }

      toggle.checked = Boolean(payload.is_published);
    } catch (error) {
      toggle.checked = !nextState;
      window.alert(error instanceof Error ? error.message : "公開状態の更新に失敗しました。");
    } finally {
      toggle.disabled = false;
    }
  });
});
