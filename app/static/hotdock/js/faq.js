document.addEventListener("DOMContentLoaded", () => {
  const buttons = document.querySelectorAll("[data-faq-button]");

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const item = button.closest("[data-faq-item]");
      const panel = item?.querySelector("[data-faq-panel]");
      const expanded = button.getAttribute("aria-expanded") === "true";

      button.setAttribute("aria-expanded", String(!expanded));
      if (panel) {
        panel.hidden = expanded;
      }

      const icon = button.querySelector(".faq-icon");
      if (icon) {
        icon.textContent = expanded ? "+" : "-";
      }
    });
  });
});
