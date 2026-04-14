document.addEventListener("DOMContentLoaded", () => {
  const toggle = document.querySelector("[data-app-sidebar-toggle]");
  const sidebar = document.querySelector("[data-app-sidebar]");

  if (!toggle || !sidebar) {
    return;
  }

  toggle.addEventListener("click", () => {
    sidebar.classList.toggle("is-open");
  });

  document.querySelectorAll("[data-row-link]").forEach((row) => {
    row.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (target.closest("a, button, input, label, select, textarea")) {
        return;
      }
      const href = row.getAttribute("data-row-link");
      if (href) {
        window.location.href = href;
      }
    });
  });

  const setAccordionState = (trigger, row, details, expanded) => {
    trigger?.setAttribute("aria-expanded", String(expanded));
    row?.setAttribute("aria-expanded", String(expanded));
    if (details) {
      details.hidden = !expanded;
    }
  };

  document.querySelectorAll("[data-accordion-toggle]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const targetId = button.getAttribute("data-accordion-target");
      if (!targetId) {
        return;
      }
      const details = document.getElementById(targetId);
      const row = button.closest("[data-accordion-row]");
      const expanded = button.getAttribute("aria-expanded") === "true";
      setAccordionState(button, row, details, !expanded);
    });
  });

  document.querySelectorAll("[data-accordion-row]").forEach((row) => {
    row.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (target.closest("button, a, input, label")) {
        return;
      }
      const targetId = row.getAttribute("data-accordion-target");
      if (!targetId) {
        return;
      }
      const details = document.getElementById(targetId);
      const trigger = row.querySelector("[data-accordion-toggle]");
      const expanded = row.getAttribute("aria-expanded") === "true";
      setAccordionState(trigger, row, details, !expanded);
    });
  });
});
