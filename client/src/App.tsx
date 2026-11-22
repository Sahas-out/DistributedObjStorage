import { useEffect, useState } from "react";
import { controllerHealthy } from "./api/dashboard";
import { usePolling } from "./hooks/usePolling";
import { DashboardPage } from "./pages/DashboardPage";
import { ObjectsPage } from "./pages/ObjectsPage";

type Tab = "objects" | "dashboard";

function tabFromHash(): Tab {
  return window.location.hash === "#/dashboard" ? "dashboard" : "objects";
}

export function App() {
  const [tab, setTab] = useState<Tab>(tabFromHash);
  const health = usePolling(controllerHealthy, 5000);

  useEffect(() => {
    const onHash = () => setTab(tabFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const up = health.data === true;
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" aria-hidden>
            <i />
            <i />
            <i />
          </span>
          <span>Distributed Object Store</span>
        </div>
        <nav className="tabs" aria-label="Pages">
          <a href="#/objects" className={tab === "objects" ? "is-on" : ""}>
            Objects
          </a>
          <a href="#/dashboard" className={tab === "dashboard" ? "is-on" : ""}>
            Dashboard
          </a>
        </nav>
        <div className={`conn ${health.data === undefined ? "" : up ? "is-up" : "is-down"}`}>
          <span aria-hidden>{up ? "●" : "▲"}</span>
          {health.data === undefined ? "Connecting…" : up ? "Controller online" : "Controller offline"}
        </div>
      </header>
      <main>{tab === "objects" ? <ObjectsPage /> : <DashboardPage />}</main>
    </div>
  );
}
