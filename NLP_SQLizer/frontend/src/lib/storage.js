const RECENT_KEY = "nlp_sqlizer_recent_dbs";
const CURRENT_KEY = "nlp_sqlizer_current_db";

// Shape we store (no password): { id, name, type, urlMasked, partsSansPass, ts }

export function getRecent() {
  try {
    return JSON.parse(localStorage.getItem(RECENT_KEY) || "[]");
  } catch {
    return [];
  }
}

export function saveRecent(entry) {
  const list = getRecent();
  const withoutDupes = [entry, ...list.filter((x) => x.id !== entry.id)];
  const trimmed = withoutDupes.slice(0, 3);
  localStorage.setItem(RECENT_KEY, JSON.stringify(trimmed));
  return trimmed;
}

export function setCurrent(entry) {
  localStorage.setItem(CURRENT_KEY, JSON.stringify(entry));
}

export function getCurrent() {
  try {
    return JSON.parse(localStorage.getItem(CURRENT_KEY) || "null");
  } catch {
    return null;
  }
}

export function clearCurrent() {
  localStorage.removeItem(CURRENT_KEY);
}

export function deleteRecent(id) {
  const list = getRecent().filter((x) => x.id !== id);
  localStorage.setItem(RECENT_KEY, JSON.stringify(list));
  const cur = getCurrent();
  if (cur?.id === id) clearCurrent();
  return list;
}
