// Build a DB URL from parts and safely encode the password.
export function buildUrl(p) {
  if (!p) return null;
  const driver = p.DB_DRIVER || "postgresql+psycopg";
  
  // SQLite uses file path, not host:port
  if (driver.includes("sqlite")) {
    const name = p.DB_NAME || "";
    // If name is a path, use it directly; otherwise treat as filename
    if (name.startsWith("/") || name.includes("\\") || name.includes(":")) {
      return `${driver}:///${name}`;
    }
    return `${driver}:///${name}`;
  }
  
  const host = p.DB_HOST || "localhost";
  const port = p.DB_PORT != null && p.DB_PORT !== "" ? String(p.DB_PORT) : "";
  const name = p.DB_NAME || "";
  const user = p.DB_USER || "";
  const pass = p.DB_PASSWORD != null ? encodeURIComponent(p.DB_PASSWORD) : "";
  const auth = user ? `${user}${pass ? ":" + pass : ""}@` : "";
  
  // MongoDB format: mongodb://[user:pass@]host[:port]/[database] or mongodb+srv://
  if (driver === "mongodb") {
    // For MongoDB, port is optional and database can be in query params
    const portPart = port ? `:${port}` : "";
    // If no database name, just return connection string without trailing slash
    if (!name) {
      return `${driver}://${auth}${host}${portPart}`;
    }
    return `${driver}://${auth}${host}${portPart}/${name}`;
  }
  
  // Standard format: driver://user:pass@host:port/database
  const portPart = port ? `:${port}` : "";
  return `${driver}://${auth}${host}${portPart}/${name}`;
}

// Quick-and-dirty parser for driver://user:pass@host:port/db
export function parseUrl(url) {
  if (!url) return null;
  
  // SQLite format: sqlite:///path/to/db or sqlite:///C:/path/to/db
  if (url.startsWith("sqlite:///")) {
    const path = url.replace(/^sqlite:\/\/\//, "");
    return {
      DB_DRIVER: "sqlite+pysqlite",
      DB_HOST: "",
      DB_PORT: undefined,
      DB_NAME: path,
      DB_USER: undefined,
      DB_PASSWORD: undefined,
    };
  }
  
  // MongoDB format (including SRV): mongodb:// or mongodb+srv://
  if (url.startsWith("mongodb://") || url.startsWith("mongodb+srv://")) {
    const urlObj = new URL(url);
    const driver = url.startsWith("mongodb+srv://") ? "mongodb+srv" : "mongodb";
    
    // Extract database from path or query params
    let dbName = urlObj.pathname ? urlObj.pathname.replace(/^\//, "").split("/")[0] : "";
    if (!dbName) {
      // Try to get from query params
      dbName = urlObj.searchParams.get("defaultDatabase") || 
               urlObj.searchParams.get("authSource") || 
               "";
    }
    
    return {
      DB_DRIVER: driver,
      DB_HOST: urlObj.hostname || "",
      DB_PORT: urlObj.port ? Number(urlObj.port) : undefined,
      DB_NAME: dbName,
      DB_USER: urlObj.username || undefined,
      DB_PASSWORD: urlObj.password ? decodeURIComponent(urlObj.password) : undefined,
    };
  }
  
  // Standard format: driver://user:pass@host:port/database
  const m = url.match(
    /^(?<driver>[^:]+):\/\/(?:(?<user>[^:@\/?#]+)(?::(?<password>[^@]*))?@)?(?<host>[^:\/?#]+)?(?::(?<port>\d+))?\/(?<database>[^?]+)/
  );
  if (!m) {
    // Try without database in path (for URLs like driver://user:pass@host:port)
    const m2 = url.match(
      /^(?<driver>[^:]+):\/\/(?:(?<user>[^:@\/?#]+)(?::(?<password>[^@]*))?@)?(?<host>[^:\/?#]+)?(?::(?<port>\d+))?/
    );
    if (m2) {
      const g2 = m2.groups || {};
      return {
        DB_DRIVER: g2.driver,
        DB_HOST: g2.host,
        DB_PORT: g2.port ? Number(g2.port) : undefined,
        DB_NAME: "",
        DB_USER: g2.user,
        DB_PASSWORD: g2.password ? decodeURIComponent(g2.password) : undefined,
      };
    }
    return null;
  }
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
  // For MongoDB, use host as part of ID even if DB_NAME is empty
  const dbName = p?.DB_NAME || "";
  const host = p?.DB_HOST || "localhost";
  return `${t}:${host}:${p?.DB_PORT || ""}:${dbName}`;
}

export function driverPretty(driver) {
  if (!driver) return "Unknown";
  const base = driver.split("+")[0];
  const map = {
    postgresql: "PostgreSQL",
    mysql: "MySQL",
    sqlite: "SQLite",
    mongodb: "MongoDB",
    mssql: "SQL Server",
    oracle: "Oracle",
  };
  return map[base] || base.toUpperCase();
}
