// Build a DB URL from parts and safely encode the password.
export function buildUrl(p) {
  if (!p) return null;
  const driver = p.DB_DRIVER || "postgresql+psycopg";
  const host = p.DB_HOST || "localhost";
  const port = p.DB_PORT != null ? String(p.DB_PORT) : "5432";
  const name = p.DB_NAME || "";
  const user = p.DB_USER || "";
  const pass = p.DB_PASSWORD != null ? encodeURIComponent(p.DB_PASSWORD) : "";
  const auth = user ? `${user}${pass ? ":" + pass : ""}@` : "";
  return `${driver}://${auth}${host}:${port}/${name}`;
}

// Quick-and-dirty parser for driver://user:pass@host:port/db
export function parseUrl(url) {
  const m = url.match(
    /^(?<driver>[^:]+):\/\/(?:(?<user>[^:@\/?#]+)(?::(?<password>[^@]*))?@)?(?<host>[^:\/?#]+)?(?::(?<port>\d+))?\/(?<database>[^?]+)/
  );
  if (!m) return null;
  const g = m.groups || {};
  return {
    DB_DRIVER: g.driver,
    DB_HOST: g.host,
    DB_PORT: g.port ? Number(g.port) : undefined,
    DB_NAME: g.database,
    DB_USER: g.user,
    DB_PASSWORD: g.password ? decodeURIComponent(g.password) : undefined,
  };
}

// Friendly ID for storage + display
export function connectionId(partsOrUrl) {
  const p = typeof partsOrUrl === "string" ? parseUrl(partsOrUrl) : partsOrUrl;
  const t = (p?.DB_DRIVER || "").split("+")[0]; // "postgresql", "mysql", etc.
  return `${t}:${p?.DB_HOST || "localhost"}:${p?.DB_PORT || ""}:${
    p?.DB_NAME || ""
  }`;
}

export function driverPretty(driver) {
  if (!driver) return "Unknown";
  const base = driver.split("+")[0];
  const map = {
    postgresql: "PostgreSQL",
    mysql: "MySQL",
    sqlite: "SQLite",
    mssql: "SQL Server",
  };
  return map[base] || base.toUpperCase();
}
