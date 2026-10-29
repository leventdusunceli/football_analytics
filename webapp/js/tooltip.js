// One floating tooltip shared by the pitch maps (hover or tap a marker)
// and the stat-term buttons (hover, keyboard focus or tap a term).

const tooltip = document.getElementById("tooltip");
let pinnedOwner = null;

// `content` is an HTML string built from our own precomputed data (map
// markers) or a DOM node (term explanations, inserted as plain text).
export function showTooltip(clientX, clientY, content) {
  tooltip.replaceChildren();
  if (typeof content === "string") tooltip.innerHTML = content;
  else tooltip.append(content);
  tooltip.hidden = false;
  const { width, height } = tooltip.getBoundingClientRect();
  const left = Math.min(clientX + 12, window.innerWidth - width - 8);
  const top = clientY + 12 + height > window.innerHeight ? clientY - height - 12 : clientY + 12;
  tooltip.style.left = `${Math.max(8, left)}px`;
  tooltip.style.top = `${Math.max(8, top)}px`;
}

export function hideTooltip() {
  tooltip.hidden = true;
  pinnedOwner = null;
  document.querySelectorAll("[aria-describedby='tooltip']").forEach((el) => el.removeAttribute("aria-describedby"));
  document.querySelectorAll(".pitch-hover").forEach((el) => el.remove());
}

// A pinned tooltip (opened by a click or tap) stays until the user taps
// elsewhere, scrolls, or presses Escape; hover tooltips follow the mouse.
export function pinTooltip(owner) {
  pinnedOwner = owner;
}

export function isTooltipPinned(owner = null) {
  return owner ? pinnedOwner === owner : pinnedOwner !== null;
}

document.addEventListener("click", (event) => {
  if (pinnedOwner && !pinnedOwner.contains(event.target)) hideTooltip();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !tooltip.hidden) hideTooltip();
});
// The tooltip is fixed to the screen, so it would drift off its target.
window.addEventListener("scroll", () => !tooltip.hidden && hideTooltip(), { passive: true });

// ------------------------------------------------------------------ //
// Stat-term explanations                                               //
// ------------------------------------------------------------------ //

// Markup for a term with an explanation, e.g. termButton("xG"). Terms are
// buttons so they can be reached with Tab and tapped on touch screens.
export function termButton(term, label = term) {
  return `<button type="button" class="term" data-term="${term}">${label}</button>`;
}

// Call once: every .term button on the page, now or added later, shows
// its glossary entry on mouse hover, keyboard focus, or tap.
export function enableTermTips(glossary) {
  const show = (button) => {
    const text = glossary[button.dataset.term];
    if (!text) return;
    const rect = button.getBoundingClientRect();
    showTooltip(rect.left - 12, rect.bottom - 6, document.createTextNode(text));
    button.setAttribute("aria-describedby", "tooltip");
  };
  const termFrom = (event) => event.target.closest?.(".term");

  document.addEventListener("pointerover", (event) => {
    const button = termFrom(event);
    if (button && event.pointerType === "mouse" && !isTooltipPinned()) show(button);
  });
  document.addEventListener("pointerout", (event) => {
    if (termFrom(event) && event.pointerType === "mouse" && !isTooltipPinned()) hideTooltip();
  });
  document.addEventListener("focusin", (event) => {
    const button = termFrom(event);
    if (button) show(button);
  });
  document.addEventListener("focusout", (event) => {
    if (termFrom(event) && !isTooltipPinned()) hideTooltip();
  });
  document.addEventListener("click", (event) => {
    const button = termFrom(event);
    if (!button) return;
    if (isTooltipPinned(button)) return hideTooltip();
    show(button);
    pinTooltip(button);
  });
}
