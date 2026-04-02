const articleBody = document.querySelector("[data-article-body]");
const toc = document.querySelector("[data-article-toc]");
const tocLinksRoot = document.querySelector("[data-article-toc-links]");

if (articleBody && toc && tocLinksRoot) {
  const headings = Array.from(articleBody.querySelectorAll("h2, h3"));

  const slugifyHeading = (value) =>
    value
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^\p{Letter}\p{Number}\s-]/gu, "")
      .trim()
      .replace(/[\s-]+/g, "-");

  const usedIds = new Set(
    Array.from(document.querySelectorAll("[id]"))
      .map((element) => element.id)
      .filter(Boolean),
  );

  const ensureHeadingId = (heading, index) => {
    if (heading.id) {
      usedIds.add(heading.id);
      return heading.id;
    }

    const baseId = slugifyHeading(heading.textContent || "") || `section-${index + 1}`;
    let nextId = baseId;
    let suffix = 2;
    while (usedIds.has(nextId)) {
      nextId = `${baseId}-${suffix}`;
      suffix += 1;
    }

    heading.id = nextId;
    usedIds.add(nextId);
    return nextId;
  };

  if (headings.length === 0) {
    toc.hidden = true;
  } else {
    const links = headings.map((heading, index) => {
      const id = ensureHeadingId(heading, index);
      const link = document.createElement("a");
      link.href = `#${id}`;
      link.textContent = (heading.textContent || "").trim();
      if (heading.tagName === "H3") {
        link.classList.add("toc-h3");
      }
      tocLinksRoot.appendChild(link);
      return link;
    });

    const setActiveLink = (id) => {
      links.forEach((link) => {
        link.classList.toggle("active", link.getAttribute("href") === `#${id}`);
      });
    };

    setActiveLink(headings[0].id);
    toc.hidden = false;

    const observer = new IntersectionObserver(
      (entries) => {
        const visibleEntries = entries
          .filter((entry) => entry.isIntersecting)
          .sort((left, right) => left.boundingClientRect.top - right.boundingClientRect.top);

        if (visibleEntries.length > 0) {
          setActiveLink(visibleEntries[0].target.id);
        }
      },
      {
        rootMargin: "-40% 0px -55% 0px",
        threshold: [0, 1],
      },
    );

    headings.forEach((heading) => observer.observe(heading));
  }
}
