const FILES = {
  summary: "./processed/summary.json",
  timeline: "./processed/timeline_monthly.json",
  topArtists: "./processed/top_artists.json",
  topTracks: "./processed/top_tracks.json",
  yearly: "./processed/yearly_breakdown.json",
  chapters: "./processed/chapters.json",
  wonders: "./processed/one_stream_wonders.json",
  comebacks: "./processed/comeback_arcs.json",
  personality: "./processed/personality.json",
};

function byId(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString();
}

function formatHours(value, digits = 1) {
  return `${Number(value || 0).toFixed(digits)}h`;
}

function formatPct(decimal, digits = 1) {
  return `${(Number(decimal || 0) * 100).toFixed(digits)}%`;
}

function formatHour(hour) {
  const h = Number(hour) || 0;
  return `${String(h).padStart(2, "0")}:00`;
}

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to load ${path} (${response.status})`);
  }
  return response.json();
}

function setStatus(message, isError = false) {
  const statusCard = byId("statusCard");
  if (!statusCard) {
    return;
  }
  statusCard.textContent = message;
  statusCard.style.borderColor = isError ? "rgba(231,111,81,.7)" : "rgba(255,255,255,.16)";
  statusCard.style.background = isError ? "rgba(231,111,81,.16)" : "rgba(255,255,255,.08)";
}

function renderHero(summary, personality) {
  const subtitle = byId("heroSubtitle");
  const heroKpis = byId("heroKpis");

  subtitle.textContent = `From ${summary.dateRange.startLabel} to ${summary.dateRange.endLabel} (${summary.dateRange.spanYears.toFixed(
    1,
  )} years), this archive tells a ${personality.headline.toLowerCase()} story.`;

  const pills = [
    `${formatHours(summary.totals.totalHours, 0)} total listening`,
    `${formatNumber(summary.totals.qualifiedEvents)} qualified streams`,
    `${formatNumber(summary.totals.uniqueArtists)} artists`,
    `${formatPct(summary.behavior.nightShare)} after 10pm`,
  ];

  heroKpis.innerHTML = pills.map((pill) => `<span class="kpi-pill">${escapeHtml(pill)}</span>`).join("");
}

function renderStats(summary, chapters) {
  const statsGrid = byId("statsGrid");
  const peakMonth = summary.milestones.peakMonth;
  const quietMonth = summary.milestones.quietMonth;

  const cards = [
    {
      label: "Total Listening",
      value: formatHours(summary.totals.totalHours, 0),
      note: "Across all audio and video sessions",
    },
    {
      label: "Music Hours",
      value: formatHours(summary.totals.trackHours, 0),
      note: "Track-only listening time",
    },
    {
      label: "Podcast + Video",
      value: formatHours(summary.totals.podcastHours + summary.totals.videoHours, 0),
      note: "Long-form sessions included",
    },
    {
      label: "Unique Artists",
      value: formatNumber(summary.totals.uniqueArtists),
      note: `${formatNumber(summary.totals.uniqueTracks)} unique tracks`,
    },
    {
      label: "Active Listening Days",
      value: formatNumber(summary.totals.activeDays),
      note: `${summary.totals.longestStreakDays} day longest streak`,
    },
    {
      label: "Top Listening Hour",
      value: formatHour(summary.behavior.topListeningHour),
      note: `${formatPct(summary.behavior.nightShare)} of listening after 10pm`,
    },
    {
      label: "One-Stream Wonders",
      value: formatNumber(summary.totals.oneStreamWonders),
      note: "Single-play memories that still count",
    },
    {
      label: "Story Chapters",
      value: formatNumber(chapters.length),
      note: `${peakMonth?.label || "N/A"} was your loudest month`,
    },
    {
      label: "Skip Rate",
      value: formatPct(summary.behavior.skipRate),
      note: `${formatPct(summary.behavior.shuffleRate)} shuffle usage`,
    },
    {
      label: "Quietest Month",
      value: quietMonth?.label || "N/A",
      note: quietMonth ? formatHours(quietMonth.hours, 1) : "No data",
    },
  ];

  statsGrid.innerHTML = cards
    .map(
      (card) => `
      <article class="stat-card">
        <p class="stat-label">${escapeHtml(card.label)}</p>
        <p class="stat-value">${escapeHtml(card.value)}</p>
        <p class="stat-note">${escapeHtml(card.note)}</p>
      </article>
    `,
    )
    .join("");
}

function renderTimeline(timeline, summary) {
  const chart = byId("timelineChart");
  const wrap = chart.closest(".timeline-wrap");
  const peakKey = summary.milestones.peakMonth?.monthKey;
  const maxHours = Math.max(...timeline.map((item) => item.hours), 1);
  const tooltipId = "timelineTooltip";

  let tooltip = document.getElementById(tooltipId);
  if (!tooltip) {
    tooltip = document.createElement("div");
    tooltip.id = tooltipId;
    tooltip.className = "timeline-tooltip";
    document.body.appendChild(tooltip);
  }

  const hideTooltip = () => {
    tooltip.classList.remove("visible");
  };

  const placeTooltip = (bar, label, clientX) => {
    tooltip.textContent = label;
    tooltip.classList.add("visible");

    const barRect = bar.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
    const viewportPad = 10;
    const anchorX = Number.isFinite(clientX) ? clientX : barRect.left + barRect.width / 2;

    let left = anchorX - tooltipRect.width / 2;
    left = Math.max(viewportPad, Math.min(left, window.innerWidth - tooltipRect.width - viewportPad));

    let top = barRect.top - tooltipRect.height - 10;
    if (top < viewportPad) {
      top = barRect.bottom + 10;
    }

    tooltip.style.left = `${Math.round(left)}px`;
    tooltip.style.top = `${Math.round(top)}px`;
  };

  chart.innerHTML = "";
  timeline.forEach((month, index) => {
    const bar = document.createElement("div");
    bar.className = "timeline-bar";
    bar.tabIndex = 0;
    bar.style.setProperty("--h", String(month.hours / maxHours));
    bar.dataset.label = `${month.label} • ${month.hours.toFixed(1)}h • ${month.trackStreams} tracks`;
    bar.setAttribute("aria-label", bar.dataset.label);

    const show = (event) => {
      placeTooltip(bar, bar.dataset.label, event?.clientX);
    };

    bar.addEventListener("mouseenter", show);
    bar.addEventListener("mousemove", show);
    bar.addEventListener("focus", show);
    bar.addEventListener("mouseleave", hideTooltip);
    bar.addEventListener("blur", hideTooltip);

    if (month.monthKey === peakKey) {
      bar.dataset.type = "peak";
    }
    bar.style.animationDelay = `${Math.min(index * 0.01, 0.6)}s`;
    chart.appendChild(bar);
  });

  if (!tooltip.dataset.bound) {
    wrap?.addEventListener("scroll", hideTooltip, { passive: true });
    window.addEventListener("scroll", hideTooltip, { passive: true });
    window.addEventListener("resize", hideTooltip, { passive: true });
    tooltip.dataset.bound = "true";
  }
}

function renderRankedList(containerId, items, kind) {
  const container = byId(containerId);
  const maxValue = Math.max(...items.map((item) => item.streams), 1);

  container.innerHTML = `<div class="rank-list">${items
    .slice(0, 14)
    .map((item) => {
      const ratio = item.streams / maxValue;
      const title = kind === "artist" ? item.artist : item.track;
      const meta =
        kind === "artist"
          ? `${item.streams} plays • ${item.activeYears} active years • ${item.mood}`
          : `${item.artist} • ${item.streams} plays • ${item.firstSeen ?? ""}`;
      const value = kind === "artist" ? formatHours(item.hours, 1) : `${item.streams} plays`;

      return `
      <div class="rank-row" style="background: linear-gradient(90deg, rgba(255,255,255,.08) ${
        ratio * 30
      }%, rgba(255,255,255,.03) ${Math.max(35, ratio * 30 + 5)}%);">
        <span class="rank-num">${item.rank}</span>
        <div class="rank-main">
          <p class="rank-title">${escapeHtml(title)}</p>
          <p class="rank-meta">${escapeHtml(meta)}</p>
        </div>
        <p class="rank-value">${escapeHtml(value)}</p>
      </div>`;
    })
    .join("")}</div>`;
}

function renderChapters(chapters) {
  const root = byId("chapterCards");
  root.innerHTML = chapters
    .map(
      (chapter) => `
      <article class="chapter-card" data-kind="${escapeHtml(chapter.kind)}">
        <p class="chapter-kicker">${escapeHtml(chapter.when)}</p>
        <h4 class="chapter-title">${escapeHtml(chapter.title)}</h4>
        <p class="chapter-subtitle">${escapeHtml(chapter.subtitle)}</p>
        <p class="chapter-body">${escapeHtml(chapter.body)}</p>
        <div class="chapter-foot">
          <span>${escapeHtml(chapter.metricLabel)}</span>
          <strong>${escapeHtml(chapter.metricValue)}</strong>
        </div>
      </article>
    `,
    )
    .join("");
}

function renderYearExplorer(yearly) {
  const select = byId("yearSelect");
  const summary = byId("yearSummary");

  const sortedYears = [...yearly].sort((a, b) => b.year - a.year);
  select.innerHTML = sortedYears
    .map((entry) => `<option value="${entry.year}">${entry.year}</option>`)
    .join("");

  const renderYear = (year) => {
    const entry = sortedYears.find((item) => item.year === Number(year));
    if (!entry) {
      return;
    }

    const topArtists = entry.topArtists
      .slice(0, 3)
      .map((artist) => `${artist.artist} (${artist.hours.toFixed(1)}h)`)
      .join(" • ");
    const topTracks = entry.topTracks
      .slice(0, 3)
      .map((track) => `${track.track} - ${track.artist}`)
      .join(" • ");

    summary.innerHTML = `
      <div class="year-chip-wrap">
        <span class="year-chip">${formatHours(entry.hours, 0)} total</span>
        <span class="year-chip">${formatNumber(entry.streams)} streams</span>
        <span class="year-chip">${formatNumber(entry.uniqueArtists)} artists</span>
        <span class="year-chip">${formatNumber(entry.uniqueTracks)} tracks</span>
        <span class="year-chip">${formatPct(entry.nightShare)} at night</span>
      </div>
      <div class="list-item">
        <p class="list-item-title">Peak month: ${escapeHtml(entry.busiestMonth?.label || "N/A")}</p>
        <p class="list-item-sub">${entry.busiestMonth ? formatHours(entry.busiestMonth.hours, 1) : "No data"}</p>
      </div>
      <div class="list-item">
        <p class="list-item-title">Top artists</p>
        <p class="list-item-sub">${escapeHtml(topArtists || "No data")}</p>
      </div>
      <div class="list-item">
        <p class="list-item-title">Top tracks</p>
        <p class="list-item-sub">${escapeHtml(topTracks || "No data")}</p>
      </div>
    `;
  };

  select.addEventListener("change", () => renderYear(select.value));
  renderYear(sortedYears[0]?.year);
}

function renderPersonality(personality) {
  const root = byId("personalityGrid");
  root.innerHTML = `
    <p class="personality-headline">${escapeHtml(personality.headline)}</p>
    ${personality.traits
      .map(
        (trait) => `
        <article class="trait-card">
          <div class="trait-top">
            <p class="trait-name">${escapeHtml(trait.label)}</p>
            <p class="trait-score">${trait.score}/100</p>
          </div>
          <div class="trait-bar" style="--score:${trait.score};"><span></span></div>
          <p class="trait-value">${escapeHtml(trait.value)}</p>
        </article>
      `,
      )
      .join("")}
  `;
}

function renderWonders(wonders, summary) {
  const root = byId("wonderList");
  root.innerHTML = wonders
    .slice(0, 16)
    .map(
      (item) => `
      <article class="list-item">
        <p class="list-item-title">${escapeHtml(item.track)} - ${escapeHtml(item.artist)}</p>
        <p class="list-item-sub">${escapeHtml(item.playedAt)} • ${formatHours(item.hours, 2)} listened</p>
        <p class="list-item-note">${escapeHtml(item.memoryPrompt)}</p>
      </article>
    `,
    )
    .join("");

  const panelSubtext = document.querySelector("#wonderList")?.closest("article")?.querySelector(".panel-subtext");
  if (panelSubtext) {
    panelSubtext.textContent = `${formatNumber(
      summary.totals.oneStreamWonders,
    )} tracks were streamed once (showing top sample).`;
  }
}

function renderComebacks(comebacks) {
  const root = byId("comebackList");
  root.innerHTML = comebacks
    .slice(0, 12)
    .map(
      (item) => `
      <article class="list-item">
        <p class="list-item-title">${escapeHtml(item.artist)}</p>
        <p class="list-item-sub">${Math.round(item.gapDays / 30)} months away • returned in ${item.returnYear}</p>
        <p class="list-item-note">${escapeHtml(item.narrative)}</p>
      </article>
    `,
    )
    .join("");
}

async function init() {
  try {
    setStatus("Loading processed story data...");

    const [summary, timeline, topArtists, topTracks, yearly, chapters, wonders, comebacks, personality] =
      await Promise.all([
        fetchJson(FILES.summary),
        fetchJson(FILES.timeline),
        fetchJson(FILES.topArtists),
        fetchJson(FILES.topTracks),
        fetchJson(FILES.yearly),
        fetchJson(FILES.chapters),
        fetchJson(FILES.wonders),
        fetchJson(FILES.comebacks),
        fetchJson(FILES.personality),
      ]);

    renderHero(summary, personality);
    renderStats(summary, chapters);
    renderTimeline(timeline, summary);
    renderRankedList("topArtistsTable", topArtists, "artist");
    renderRankedList("topTracksTable", topTracks, "track");
    renderChapters(chapters);
    renderYearExplorer(yearly);
    renderPersonality(personality);
    renderWonders(wonders, summary);
    renderComebacks(comebacks);

    setStatus(
      `Loaded ${formatNumber(summary.totals.events)} events from ${summary.sourceFiles.length} files (${summary.timezone} analysis).`,
    );
  } catch (error) {
    const fileMode = window.location.protocol === "file:";
    const localHint = fileMode
      ? " Open this project with a local server (for example: `python3 -m http.server 4173`) and then visit http://localhost:4173."
      : "";
    setStatus(`Could not load dashboard data. ${error.message}.${localHint}`, true);
  }
}

init();
