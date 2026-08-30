(() => {
  const API = "/api/v9/dashboard?limit=50";

  const fmtDelta = (v) => {
    const n = Number(v || 0);
    if (n === 0) return "±0.0";
    return `${n > 0 ? "+" : ""}${n.toFixed(1)}`;
  };

  const badgeClass = (label) => {
    if (label === "急加速") return "v9-hot";
    if (label === "加速中") return "v9-accelerating";
    if (label === "上昇中") return "v9-rising";
    return "v9-watching";
  };

  function ensureStyles() {
    if (document.getElementById("v9-ui-style")) return;

    const style = document.createElement("style");
    style.id = "v9-ui-style";

    style.textContent = `
      .v9-strip {
        margin: 14px 0 18px;
        padding: 14px;
        border: 1px solid rgba(255,255,255,.12);
        border-radius: 16px;
        background: rgba(255,255,255,.035);
      }

      .v9-strip-head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-bottom: 10px;
      }

      .v9-strip-title {
        font-size: 13px;
        font-weight: 800;
        letter-spacing: .08em;
      }

      .v9-strip-note {
        font-size: 11px;
        opacity: .55;
      }

      .v9-cards {
        display: flex;
        gap: 8px;
        overflow: auto;
        padding-bottom: 2px;
      }

      .v9-card {
        min-width: 178px;
        padding: 12px;
        border-radius: 14px;
        background: rgba(255,255,255,.055);
        border: 1px solid rgba(255,255,255,.08);
        text-decoration: none;
        color: inherit;
      }

      .v9-keyword {
        font-size: 15px;
        font-weight: 800;
        margin: 6px 0 10px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .v9-badge {
        display: inline-flex;
        align-items: center;
        padding: 4px 8px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 900;
      }

      .v9-hot {
        background: #ff4d4d;
        color: #fff;
      }

      .v9-accelerating {
        background: #ffb000;
        color: #111;
      }

      .v9-rising {
        background: #d7ff38;
        color: #111;
      }

      .v9-watching {
        background: rgba(255,255,255,.12);
        color: #ddd;
      }

      .v9-metrics {
        display: grid;
        grid-template-columns: repeat(3,1fr);
        gap: 5px;
      }

      .v9-metric {
        padding: 6px 4px;
        border-radius: 8px;
        background: rgba(0,0,0,.22);
        text-align: center;
      }

      .v9-metric b {
        display: block;
        font-size: 12px;
      }

      .v9-metric span {
        font-size: 9px;
        opacity: .55;
      }

      .v9-source {
        font-size: 10px;
        opacity: .55;
        margin-top: 8px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      @media(min-width:900px) {
        .v9-card {
          min-width: 210px;
        }

        .v9-strip {
          padding: 16px;
        }
      }
    `;

    document.head.appendChild(style);
  }

  async function loadV9() {
    try {
      const res = await fetch(API, { cache: "no-store" });

      if (!res.ok) return;

      const data = await res.json();
      const items = (data.items || []).slice(0, 8);

      if (!items.length) return;

      ensureStyles();

      const host =
        document.querySelector(".prebuzz-hero") ||
        document.querySelector("main") ||
        document.body;

      if (document.getElementById("v9-live-strip")) return;

      const section = document.createElement("section");

      section.id = "v9-live-strip";
      section.className = "v9-strip";

      section.innerHTML = `
        <div class="v9-strip-head">
          <div class="v9-strip-title">
            ⚡ REAL VELOCITY / 実データ加速度
          </div>

          <div class="v9-strip-note">
            30分・1時間・3時間
          </div>
        </div>

        <div class="v9-cards">

          ${items.map(x => `
            <a
              class="v9-card"
              href="/trend/${encodeURIComponent(x.slug)}"
            >

              <span class="v9-badge ${badgeClass(x.velocity_label)}">
                ${x.velocity_label || "観測中"}
              </span>

              <div class="v9-keyword">
                ${x.keyword}
              </div>

              <div class="v9-metrics">

                <div class="v9-metric">
                  <b>${fmtDelta(x.velocity_30m)}</b>
                  <span>30 MIN</span>
                </div>

                <div class="v9-metric">
                  <b>${fmtDelta(x.velocity_1h)}</b>
                  <span>1 HOUR</span>
                </div>

                <div class="v9-metric">
                  <b>${fmtDelta(x.velocity_3h)}</b>
                  <span>3 HOURS</span>
                </div>

              </div>

              <div class="v9-source">
                FIRST SIGNAL: ${x.first_source || "観測中"}
              </div>

            </a>
          `).join("")}

        </div>
      `;

      host.insertAdjacentElement("afterend", section);

    } catch (e) {
      console.warn("V9 dashboard:", e);
    }
  }

  document.addEventListener("DOMContentLoaded", loadV9);
})();
