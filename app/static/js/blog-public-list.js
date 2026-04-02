const blogSearchInput = document.querySelector("[data-blog-search]");
const blogRows = Array.from(document.querySelectorAll("[data-blog-row]"));
const blogEmptyState = document.querySelector("[data-blog-empty]");

const syncBlogEmptyState = () => {
  if (!blogEmptyState || !blogSearchInput) {
    return;
  }

  const query = blogSearchInput.value.trim();
  const visibleCount = blogRows.filter((row) => !row.hidden).length;
  blogEmptyState.hidden = query === "" || visibleCount !== 0;
};

if (blogSearchInput && blogRows.length > 0) {
  const filterRows = () => {
    const query = blogSearchInput.value.trim().toLowerCase();

    blogRows.forEach((row) => {
      const searchIndex = (row.dataset.searchIndex || "").toLowerCase();
      row.hidden = query !== "" && !searchIndex.includes(query);
    });

    syncBlogEmptyState();
  };

  blogSearchInput.addEventListener("input", filterRows);
  filterRows();
}
