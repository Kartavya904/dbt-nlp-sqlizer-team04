import { Outlet, Link, useLocation } from "react-router-dom";

export default function App() {
  const loc = useLocation();
  return (
    <div className="app-shell">
      <header className="topbar">
        <Link to="/" className="brand">
          NLP_SQLizer
        </Link>
        <nav className="nav">
          <Link to="/" className={loc.pathname === "/" ? "active" : ""}>
            Home
          </Link>
        </nav>
      </header>
      <main className="main">
        <Outlet />
      </main>
      <footer className="foot">
        <span>© {new Date().getFullYear()} Team 04</span>
      </footer>
    </div>
  );
}
