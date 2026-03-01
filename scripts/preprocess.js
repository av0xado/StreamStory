#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(PROJECT_ROOT, "data");
const OUTPUT_DIR = path.join(PROJECT_ROOT, "processed");
const ANALYSIS_TIMEZONE = process.env.STREAM_STORY_TZ || "Europe/Dublin";

const STREAM_THRESHOLDS_MS = {
  track: 30_000,
  podcast: 180_000,
  video: 120_000,
  audiobook: 180_000,
  unknown: 30_000,
};

const MS_PER_HOUR = 3_600_000;
const MS_PER_DAY = 86_400_000;

const localPartsFormatter = new Intl.DateTimeFormat("en-GB", {
  timeZone: ANALYSIS_TIMEZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  weekday: "short",
  hourCycle: "h23",
});

const monthLabelFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: ANALYSIS_TIMEZONE,
  month: "short",
  year: "numeric",
});

const dayLabelFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: ANALYSIS_TIMEZONE,
  month: "short",
  day: "numeric",
  year: "numeric",
});

const monthNameFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: ANALYSIS_TIMEZONE,
  month: "long",
  year: "numeric",
});

const seasonTitleBank = {
  Winter: ["Frostlit Reverie", "Midnight Hibernation", "Quiet-Season Loop"],
  Spring: ["Second-Wind Bloom", "Reinvention Season", "New-Light Rotation"],
  Summer: ["Summer of Melancholy", "Sunset Spiral", "Heatwave Replay"],
  Autumn: ["Afterglow Chapters", "Leaf-Fall Reflection", "Grey-Sky Groove"],
};

const discoveryTitleBank = [
  "Rabbit-Hole Month",
  "Curiosity Bloom",
  "Discovery Detour",
  "Open-Tab Listening Era",
  "Soundtrack Exploration Weekends",
];

const comebackTitleBank = [
  "The Return Chapter",
  "Reunion Arc",
  "Familiar Ghost, New Season",
  "Back Into Rotation",
];

function pad2(value) {
  return String(value).padStart(2, "0");
}

function cleanText(value) {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.replace(/\s+/g, " ").trim();
  return normalized.length > 0 ? normalized : null;
}

function clamp(num, min, max) {
  return Math.min(max, Math.max(min, num));
}

function hashString(value) {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function pickDeterministic(list, key) {
  if (!Array.isArray(list) || list.length === 0) {
    return "";
  }
  return list[hashString(key) % list.length];
}

function getSeason(month) {
  if (month === 12 || month <= 2) {
    return "Winter";
  }
  if (month <= 5) {
    return "Spring";
  }
  if (month <= 8) {
    return "Summer";
  }
  return "Autumn";
}

function getHourMood(hour) {
  if (hour >= 0 && hour <= 4) {
    return "late-night drift";
  }
  if (hour <= 10) {
    return "morning reset";
  }
  if (hour <= 16) {
    return "afternoon focus";
  }
  if (hour <= 21) {
    return "evening wind-down";
  }
  return "night-owl ritual";
}

function toHours(ms) {
  return Number((ms / MS_PER_HOUR).toFixed(2));
}

function toPct(decimal) {
  return Number((decimal * 100).toFixed(1));
}

function topNEntries(map, n, mapper) {
  return [...map.entries()]
    .sort((a, b) => b[1].ms - a[1].ms)
    .slice(0, n)
    .map(([key, value], index) => mapper(key, value, index));
}

function getLocalParts(date) {
  const raw = localPartsFormatter.formatToParts(date);
  const parts = {};

  for (const token of raw) {
    if (token.type !== "literal") {
      parts[token.type] = token.value;
    }
  }

  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Number(parts.day);
  const hour = Number(parts.hour);
  const season = getSeason(month);
  const dateKey = `${year}-${pad2(month)}-${pad2(day)}`;
  const monthKey = `${year}-${pad2(month)}`;
  const dayOrdinal = Math.floor(Date.UTC(year, month - 1, day) / MS_PER_DAY);

  return {
    year,
    month,
    day,
    hour,
    weekday: parts.weekday,
    season,
    dateKey,
    monthKey,
    dayOrdinal,
  };
}

function getContentType(row, sourceFile) {
  const hasTrack = Boolean(row.master_metadata_track_name && row.master_metadata_album_artist_name);
  if (hasTrack) {
    return "track";
  }

  const hasEpisode = Boolean(row.episode_name || row.spotify_episode_uri);
  if (hasEpisode) {
    return sourceFile.includes("Video") ? "video" : "podcast";
  }

  const hasAudiobook = Boolean(row.audiobook_title || row.audiobook_chapter_title || row.audiobook_uri);
  if (hasAudiobook) {
    return "audiobook";
  }

  return "unknown";
}

function normalizeEvent(row, sourceFile) {
  if (!row || !row.ts) {
    return null;
  }

  const tsDate = new Date(row.ts);
  const epoch = tsDate.getTime();
  if (!Number.isFinite(epoch)) {
    return null;
  }

  const local = getLocalParts(tsDate);
  const type = getContentType(row, sourceFile);
  const trackName = cleanText(row.master_metadata_track_name);
  const artistName = cleanText(row.master_metadata_album_artist_name);
  const albumName = cleanText(row.master_metadata_album_album_name);
  const episodeName = cleanText(row.episode_name);
  const showName = cleanText(row.episode_show_name);

  let trackId = cleanText(row.spotify_track_uri);
  if (!trackId && trackName && artistName) {
    trackId = `fallback:${trackName.toLowerCase()}|${artistName.toLowerCase()}`;
  }

  const msPlayed = Number(row.ms_played) || 0;
  const streamThreshold = STREAM_THRESHOLDS_MS[type] ?? STREAM_THRESHOLDS_MS.unknown;
  const qualified = msPlayed >= streamThreshold;

  return {
    ts: row.ts,
    epoch,
    local,
    type,
    msPlayed,
    qualified,
    skipped: Boolean(row.skipped),
    shuffle: Boolean(row.shuffle),
    offline: Boolean(row.offline),
    connCountry: cleanText(row.conn_country) || "UNK",
    trackId,
    trackName,
    artistName,
    albumName,
    episodeName,
    showName,
    sourceFile,
    reasonStart: cleanText(row.reason_start),
    reasonEnd: cleanText(row.reason_end),
  };
}

function createMonthAggregate(monthKey, year, month, season) {
  return {
    monthKey,
    year,
    month,
    season,
    ms: 0,
    streams: 0,
    trackMs: 0,
    podcastMs: 0,
    videoMs: 0,
    audiobookMs: 0,
    unknownMs: 0,
    trackStreams: 0,
    podcastStreams: 0,
    videoStreams: 0,
    audiobookStreams: 0,
    unknownStreams: 0,
    uniqueArtists: new Set(),
    uniqueTracks: new Set(),
    hourMs: Array(24).fill(0),
  };
}

function createYearAggregate(year) {
  return {
    year,
    ms: 0,
    streams: 0,
    typeMs: {
      track: 0,
      podcast: 0,
      video: 0,
      audiobook: 0,
      unknown: 0,
    },
    typeStreams: {
      track: 0,
      podcast: 0,
      video: 0,
      audiobook: 0,
      unknown: 0,
    },
    skipCount: 0,
    shuffleCount: 0,
    offlineCount: 0,
    hourMs: Array(24).fill(0),
    monthMs: new Map(),
    uniqueArtists: new Set(),
    uniqueTracks: new Set(),
    artistMap: new Map(),
    trackMap: new Map(),
  };
}

function createArtistAggregate(name) {
  return {
    artist: name,
    ms: 0,
    streams: 0,
    firstEpoch: Number.POSITIVE_INFINITY,
    lastEpoch: Number.NEGATIVE_INFINITY,
    years: new Set(),
    seasons: new Set(),
    topHourMs: Array(24).fill(0),
  };
}

function createTrackAggregate(trackId, trackName, artistName, albumName) {
  return {
    trackId,
    trackName,
    artistName,
    albumName,
    ms: 0,
    streams: 0,
    firstEpoch: Number.POSITIVE_INFINITY,
    lastEpoch: Number.NEGATIVE_INFINITY,
    firstHour: null,
    firstDateLabel: null,
  };
}

function longestStreakFromOrdinals(dayOrdinals) {
  if (dayOrdinals.length === 0) {
    return 0;
  }

  const sorted = [...dayOrdinals].sort((a, b) => a - b);
  let best = 1;
  let current = 1;

  for (let i = 1; i < sorted.length; i += 1) {
    if (sorted[i] === sorted[i - 1] + 1) {
      current += 1;
      best = Math.max(best, current);
    } else if (sorted[i] !== sorted[i - 1]) {
      current = 1;
    }
  }

  return best;
}

function buildComebackArcs(artistEventsByName) {
  const arcs = [];

  for (const [artist, events] of artistEventsByName.entries()) {
    if (events.length < 15) {
      continue;
    }

    events.sort((a, b) => a.epoch - b.epoch);

    let strongestGap = null;
    for (let i = 0; i < events.length - 1; i += 1) {
      const gapMs = events[i + 1].epoch - events[i].epoch;
      const gapDays = gapMs / MS_PER_DAY;

      if (!strongestGap || gapDays > strongestGap.gapDays) {
        strongestGap = {
          gapDays,
          beforeIndex: i,
          returnIndex: i + 1,
        };
      }
    }

    if (!strongestGap || strongestGap.gapDays < 210) {
      continue;
    }

    const returnEvent = events[strongestGap.returnIndex];
    const windowEnd = returnEvent.epoch + 90 * MS_PER_DAY;

    let returnMs = 0;
    let returnStreams = 0;
    for (let i = strongestGap.returnIndex; i < events.length; i += 1) {
      if (events[i].epoch > windowEnd) {
        break;
      }
      returnMs += events[i].msPlayed;
      returnStreams += 1;
    }

    if (returnStreams < 4 || returnMs < 1.2 * MS_PER_HOUR) {
      continue;
    }

    const score = strongestGap.gapDays * Math.log1p(returnMs / 1000) * Math.log1p(returnStreams);

    arcs.push({
      artist,
      gapDays: Math.round(strongestGap.gapDays),
      returnDate: returnEvent.ts,
      returnYear: returnEvent.local.year,
      returnWindowHours: toHours(returnMs),
      returnWindowStreams: returnStreams,
      score,
      narrative: `${artist} disappeared for ${Math.round(
        strongestGap.gapDays / 30,
      )} months, then came back with ${toHours(returnMs)} hours in about 90 days.`,
    });
  }

  return arcs.sort((a, b) => b.score - a.score);
}

function buildNarrativeChapters({ seasonMap, timeline, comebackArcs }) {
  const seasonalCards = [];
  const artistEraCards = [];
  const discoveryCards = [];
  const comebackCards = [];

  const seasonalCandidates = [];
  for (const seasonEntry of seasonMap.values()) {
    if (seasonEntry.totalMs < 2 * MS_PER_HOUR) {
      continue;
    }

    let topArtist = null;
    for (const [artist, data] of seasonEntry.artistMap.entries()) {
      if (!topArtist || data.ms > topArtist.ms) {
        topArtist = {
          artist,
          ms: data.ms,
          streams: data.streams,
        };
      }
    }

    if (!topArtist) {
      continue;
    }

    const share = topArtist.ms / seasonEntry.totalMs;
    if (share < 0.12 || topArtist.streams < 4) {
      continue;
    }

    const score = (topArtist.ms / MS_PER_HOUR) + share * 25;
    seasonalCandidates.push({
      ...seasonEntry,
      topArtist,
      share,
      score,
    });
  }

  const sortedSeasonal = seasonalCandidates
    .sort((a, b) => b.score - a.score)
    .slice(0, 8);

  const seasonalArtistSet = new Set(sortedSeasonal.map((entry) => entry.topArtist.artist));

  sortedSeasonal.forEach((entry) => {
    const key = `${entry.year}-${entry.season}-${entry.topArtist.artist}`;
    const titleStem = pickDeterministic(seasonTitleBank[entry.season], key);
    const intensityLine =
      entry.share >= 0.35
        ? "That season looked almost ritualistic."
        : entry.share >= 0.25
          ? "This artist became your seasonal anchor."
          : "A clear chapter in your long listening arc.";

    seasonalCards.push({
      id: `season-${entry.year}-${entry.season}-${hashString(entry.topArtist.artist)}`,
      kind: "seasonal-obsession",
      title: titleStem,
      subtitle: `${entry.season} ${entry.year} featuring ${entry.topArtist.artist}`,
      body: `${entry.topArtist.artist} owned ${toPct(entry.share)}% of your ${entry.season.toLowerCase()} listens (${toHours(
        entry.topArtist.ms,
      )} hours across ${entry.topArtist.streams} streams). ${intensityLine}`,
      metricLabel: "Season share",
      metricValue: `${toPct(entry.share)}%`,
      when: `${entry.season} ${entry.year}`,
      score: entry.score,
    });
  });

  const artistPeakMap = new Map();
  for (const seasonEntry of seasonMap.values()) {
    for (const [artist, data] of seasonEntry.artistMap.entries()) {
      const current = artistPeakMap.get(artist);
      if (!current || data.ms > current.ms) {
        artistPeakMap.set(artist, {
          artist,
          year: seasonEntry.year,
          season: seasonEntry.season,
          ms: data.ms,
          streams: data.streams,
          seasonTotalMs: seasonEntry.totalMs,
        });
      }
    }
  }

  [...artistPeakMap.values()]
    .filter((entry) => entry.ms >= 5 * MS_PER_HOUR && !seasonalArtistSet.has(entry.artist))
    .sort((a, b) => b.ms - a.ms)
    .slice(0, 3)
    .forEach((entry) => {
      const share = entry.seasonTotalMs === 0 ? 0 : entry.ms / entry.seasonTotalMs;
      artistEraCards.push({
        id: `artist-era-${hashString(entry.artist)}-${entry.year}-${entry.season}`,
        kind: "artist-era-peak",
        title: `${entry.artist} Main Character Arc`,
        subtitle: `${entry.season} ${entry.year} was your peak chapter with ${entry.artist}`,
        body: `You logged ${toHours(entry.ms)} hours of ${entry.artist} in this season, enough to color the entire period with a ${toPct(
          share,
        )}% share of your soundtrack.`,
        metricLabel: "Peak season hours",
        metricValue: `${toHours(entry.ms)}h`,
        when: `${entry.season} ${entry.year}`,
        score: entry.ms,
      });
    });

  const discoveryCandidates = timeline
    .filter((month) => month.trackStreams >= 90 && month.novelty >= 0.62)
    .sort((a, b) => b.novelty * Math.log1p(b.trackStreams) - a.novelty * Math.log1p(a.trackStreams))
    .slice(0, 3);

  discoveryCandidates.forEach((month) => {
    const key = `${month.monthKey}-${month.uniqueTracks}`;
    const title = pickDeterministic(discoveryTitleBank, key);
    discoveryCards.push({
      id: `discovery-${month.monthKey}`,
      kind: "discovery-burst",
      title,
      subtitle: `${month.label} was an exploration spike`,
      body: `You logged ${month.uniqueTracks} unique tracks in ${month.label}, with a novelty rate of ${toPct(
        month.novelty,
      )}%. This looks like a deliberate digging phase.`,
      metricLabel: "Novelty rate",
      metricValue: `${toPct(month.novelty)}%`,
      when: month.label,
      score: month.novelty * Math.log1p(month.trackStreams),
    });
  });

  comebackArcs.slice(0, 3).forEach((arc) => {
    const title = `${pickDeterministic(comebackTitleBank, arc.artist)}: ${arc.artist}`;
    comebackCards.push({
      id: `comeback-${hashString(arc.artist)}-${arc.returnYear}`,
      kind: "comeback-arc",
      title,
      subtitle: `${arc.artist} re-entered the soundtrack in ${arc.returnYear}`,
      body: `After a ${Math.round(arc.gapDays / 30)}-month silence, you came back with ${arc.returnWindowStreams} plays and ${arc.returnWindowHours} hours in one burst.`,
      metricLabel: "Silence gap",
      metricValue: `${Math.round(arc.gapDays / 30)} months`,
      when: String(arc.returnYear),
      score: arc.score,
    });
  });

  return [
    ...seasonalCards.slice(0, 5),
    ...artistEraCards.slice(0, 3),
    ...discoveryCards.slice(0, 2),
    ...comebackCards.slice(0, 2),
  ].slice(0, 12);
}

function buildPersonality({
  totalQualifiedStreams,
  totalTrackMs,
  skipRate,
  shuffleRate,
  nightShare,
  oneStreamWonderRatio,
  topArtistShare,
  avgStreamMinutes,
}) {
  const nightScore = Math.round(clamp(nightShare * 170, 0, 100));
  const completionScore = Math.round(clamp((1 - skipRate) * 100, 0, 100));
  const explorerScore = Math.round(clamp(oneStreamWonderRatio * 130, 0, 100));
  const loyaltyScore = Math.round(clamp(topArtistShare * 100, 0, 100));
  const shuffleScore = Math.round(clamp(shuffleRate * 120, 0, 100));

  const traits = [
    {
      id: "night_owl",
      label: "Night Owl Index",
      score: nightScore,
      value: `${toPct(nightShare)}% after 10pm`,
      description: "How much of your listening lives in the late-night window.",
    },
    {
      id: "completion",
      label: "Completion Style",
      score: completionScore,
      value: `${toPct(1 - skipRate)}% finished`,
      description: "Higher means you usually let tracks run rather than skipping.",
    },
    {
      id: "explorer",
      label: "Explorer Quotient",
      score: explorerScore,
      value: `${toPct(oneStreamWonderRatio)}% one-stream tracks`,
      description: "How much your history shows one-time discoveries and experiments.",
    },
    {
      id: "loyalty",
      label: "Loyalty Core",
      score: loyaltyScore,
      value: `${toPct(topArtistShare)}% from top artists`,
      description: "How concentrated your listening is around recurring favorites.",
    },
    {
      id: "shuffle",
      label: "Shuffle DNA",
      score: shuffleScore,
      value: `${toPct(shuffleRate)}% shuffled`,
      description: "How frequently shuffle mode shapes the listening journey.",
    },
  ];

  const profile =
    nightScore >= 65 && explorerScore >= 45
      ? "The Nocturnal Explorer"
      : loyaltyScore >= 55 && completionScore >= 70
        ? "The Devoted Album Listener"
        : shuffleScore >= 55
          ? "The Serendipity Seeker"
          : "The Balanced Curator";

  return {
    headline: profile,
    avgStreamMinutes: Number(avgStreamMinutes.toFixed(1)),
    totalQualifiedStreams,
    totalTrackHours: toHours(totalTrackMs),
    traits,
  };
}

function toReadableDate(ts) {
  if (!ts) {
    return null;
  }
  const date = new Date(ts);
  if (!Number.isFinite(date.getTime())) {
    return null;
  }
  return dayLabelFormatter.format(date);
}

function main() {
  if (!fs.existsSync(DATA_DIR)) {
    throw new Error(`Data directory not found: ${DATA_DIR}`);
  }

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const inputFiles = fs
    .readdirSync(DATA_DIR)
    .filter((name) => name.endsWith(".json") && name.startsWith("Streaming_History_"))
    .sort();

  if (inputFiles.length === 0) {
    throw new Error(`No Spotify history JSON files found in: ${DATA_DIR}`);
  }

  const normalizedEvents = [];
  const sourceFileStats = [];

  for (const fileName of inputFiles) {
    const fullPath = path.join(DATA_DIR, fileName);
    const raw = JSON.parse(fs.readFileSync(fullPath, "utf8"));

    let accepted = 0;
    let rejected = 0;

    for (const row of raw) {
      const event = normalizeEvent(row, fileName);
      if (!event) {
        rejected += 1;
        continue;
      }
      normalizedEvents.push(event);
      accepted += 1;
    }

    sourceFileStats.push({
      fileName,
      rows: raw.length,
      accepted,
      rejected,
    });
  }

  normalizedEvents.sort((a, b) => a.epoch - b.epoch);

  if (normalizedEvents.length === 0) {
    throw new Error("No valid events found after normalization.");
  }

  const typeCounters = {
    track: { events: 0, qualifiedEvents: 0, ms: 0, qualifiedMs: 0 },
    podcast: { events: 0, qualifiedEvents: 0, ms: 0, qualifiedMs: 0 },
    video: { events: 0, qualifiedEvents: 0, ms: 0, qualifiedMs: 0 },
    audiobook: { events: 0, qualifiedEvents: 0, ms: 0, qualifiedMs: 0 },
    unknown: { events: 0, qualifiedEvents: 0, ms: 0, qualifiedMs: 0 },
  };

  const monthMap = new Map();
  const yearMap = new Map();
  const seasonMap = new Map();
  const artistMap = new Map();
  const trackMap = new Map();
  const artistEventsByName = new Map();
  const countryMap = new Map();

  const activeDayOrdinals = new Set();
  const listeningHourMs = Array(24).fill(0);

  let totalEvents = 0;
  let totalQualifiedEvents = 0;
  let totalMs = 0;
  let totalQualifiedMs = 0;
  let skipCount = 0;
  let shuffleCount = 0;
  let offlineCount = 0;

  for (const event of normalizedEvents) {
    totalEvents += 1;
    totalMs += event.msPlayed;

    const typeStats = typeCounters[event.type] || typeCounters.unknown;
    typeStats.events += 1;
    typeStats.ms += event.msPlayed;

    countryMap.set(event.connCountry, (countryMap.get(event.connCountry) || 0) + 1);

    if (!event.qualified) {
      continue;
    }

    totalQualifiedEvents += 1;
    totalQualifiedMs += event.msPlayed;

    typeStats.qualifiedEvents += 1;
    typeStats.qualifiedMs += event.msPlayed;

    if (event.skipped) {
      skipCount += 1;
    }
    if (event.shuffle) {
      shuffleCount += 1;
    }
    if (event.offline) {
      offlineCount += 1;
    }

    activeDayOrdinals.add(event.local.dayOrdinal);
    listeningHourMs[event.local.hour] += event.msPlayed;

    const monthKey = event.local.monthKey;
    if (!monthMap.has(monthKey)) {
      monthMap.set(
        monthKey,
        createMonthAggregate(monthKey, event.local.year, event.local.month, event.local.season),
      );
    }

    const monthEntry = monthMap.get(monthKey);
    monthEntry.ms += event.msPlayed;
    monthEntry.streams += 1;
    monthEntry.hourMs[event.local.hour] += event.msPlayed;

    const typeMsKey = `${event.type}Ms`;
    const typeStreamsKey = `${event.type}Streams`;

    if (Object.hasOwn(monthEntry, typeMsKey)) {
      monthEntry[typeMsKey] += event.msPlayed;
    } else {
      monthEntry.unknownMs += event.msPlayed;
    }

    if (Object.hasOwn(monthEntry, typeStreamsKey)) {
      monthEntry[typeStreamsKey] += 1;
    } else {
      monthEntry.unknownStreams += 1;
    }

    const year = event.local.year;
    if (!yearMap.has(year)) {
      yearMap.set(year, createYearAggregate(year));
    }

    const yearEntry = yearMap.get(year);
    yearEntry.ms += event.msPlayed;
    yearEntry.streams += 1;
    yearEntry.typeMs[event.type] += event.msPlayed;
    yearEntry.typeStreams[event.type] += 1;
    yearEntry.skipCount += event.skipped ? 1 : 0;
    yearEntry.shuffleCount += event.shuffle ? 1 : 0;
    yearEntry.offlineCount += event.offline ? 1 : 0;
    yearEntry.hourMs[event.local.hour] += event.msPlayed;
    yearEntry.monthMs.set(monthKey, (yearEntry.monthMs.get(monthKey) || 0) + event.msPlayed);

    if (event.type !== "track" || !event.trackId || !event.artistName || !event.trackName) {
      continue;
    }

    monthEntry.uniqueArtists.add(event.artistName);
    monthEntry.uniqueTracks.add(event.trackId);

    yearEntry.uniqueArtists.add(event.artistName);
    yearEntry.uniqueTracks.add(event.trackId);

    if (!artistMap.has(event.artistName)) {
      artistMap.set(event.artistName, createArtistAggregate(event.artistName));
    }

    const artistEntry = artistMap.get(event.artistName);
    artistEntry.ms += event.msPlayed;
    artistEntry.streams += 1;
    artistEntry.firstEpoch = Math.min(artistEntry.firstEpoch, event.epoch);
    artistEntry.lastEpoch = Math.max(artistEntry.lastEpoch, event.epoch);
    artistEntry.years.add(event.local.year);
    artistEntry.seasons.add(`${event.local.year}-${event.local.season}`);
    artistEntry.topHourMs[event.local.hour] += event.msPlayed;

    if (!artistEventsByName.has(event.artistName)) {
      artistEventsByName.set(event.artistName, []);
    }
    artistEventsByName.get(event.artistName).push(event);

    const yearArtist = yearEntry.artistMap.get(event.artistName) || { ms: 0, streams: 0 };
    yearArtist.ms += event.msPlayed;
    yearArtist.streams += 1;
    yearEntry.artistMap.set(event.artistName, yearArtist);

    if (!trackMap.has(event.trackId)) {
      trackMap.set(
        event.trackId,
        createTrackAggregate(event.trackId, event.trackName, event.artistName, event.albumName),
      );
    }

    const trackEntry = trackMap.get(event.trackId);
    trackEntry.ms += event.msPlayed;
    trackEntry.streams += 1;
    trackEntry.firstEpoch = Math.min(trackEntry.firstEpoch, event.epoch);
    trackEntry.lastEpoch = Math.max(trackEntry.lastEpoch, event.epoch);
    if (trackEntry.firstHour === null || event.epoch < trackEntry.firstEpoch) {
      trackEntry.firstHour = event.local.hour;
      trackEntry.firstDateLabel = dayLabelFormatter.format(new Date(event.epoch));
    }

    const yearTrack =
      yearEntry.trackMap.get(event.trackId) || {
        trackId: event.trackId,
        trackName: event.trackName,
        artistName: event.artistName,
        ms: 0,
        streams: 0,
      };
    yearTrack.ms += event.msPlayed;
    yearTrack.streams += 1;
    yearEntry.trackMap.set(event.trackId, yearTrack);

    const seasonKey = `${event.local.year}-${event.local.season}`;
    if (!seasonMap.has(seasonKey)) {
      seasonMap.set(seasonKey, {
        seasonKey,
        year: event.local.year,
        season: event.local.season,
        totalMs: 0,
        totalStreams: 0,
        artistMap: new Map(),
      });
    }

    const seasonEntry = seasonMap.get(seasonKey);
    seasonEntry.totalMs += event.msPlayed;
    seasonEntry.totalStreams += 1;
    const seasonArtist = seasonEntry.artistMap.get(event.artistName) || { ms: 0, streams: 0 };
    seasonArtist.ms += event.msPlayed;
    seasonArtist.streams += 1;
    seasonEntry.artistMap.set(event.artistName, seasonArtist);
  }

  const firstEvent = normalizedEvents[0];
  const lastEvent = normalizedEvents[normalizedEvents.length - 1];

  const timeline = [...monthMap.values()]
    .sort((a, b) => a.monthKey.localeCompare(b.monthKey))
    .map((entry) => {
      const nightMs =
        entry.hourMs[22] +
        entry.hourMs[23] +
        entry.hourMs[0] +
        entry.hourMs[1] +
        entry.hourMs[2] +
        entry.hourMs[3] +
        entry.hourMs[4];

      const novelty = entry.trackStreams === 0 ? 0 : entry.uniqueTracks.size / entry.trackStreams;

      return {
        monthKey: entry.monthKey,
        year: entry.year,
        month: entry.month,
        season: entry.season,
        label: monthLabelFormatter.format(new Date(Date.UTC(entry.year, entry.month - 1, 1))),
        hours: toHours(entry.ms),
        streams: entry.streams,
        trackHours: toHours(entry.trackMs),
        podcastHours: toHours(entry.podcastMs),
        videoHours: toHours(entry.videoMs),
        audiobookHours: toHours(entry.audiobookMs),
        trackStreams: entry.trackStreams,
        uniqueArtists: entry.uniqueArtists.size,
        uniqueTracks: entry.uniqueTracks.size,
        novelty: Number(novelty.toFixed(4)),
        nightShare: entry.ms === 0 ? 0 : Number((nightMs / entry.ms).toFixed(4)),
      };
    });

  const topArtists = topNEntries(artistMap, 200, (artistName, entry, index) => {
    const bestHour = entry.topHourMs.indexOf(Math.max(...entry.topHourMs));
    return {
      rank: index + 1,
      artist: artistName,
      hours: toHours(entry.ms),
      streams: entry.streams,
      shareOfTrackHours:
        typeCounters.track.qualifiedMs === 0
          ? 0
          : Number((entry.ms / typeCounters.track.qualifiedMs).toFixed(4)),
      firstSeen: toReadableDate(new Date(entry.firstEpoch).toISOString()),
      lastSeen: toReadableDate(new Date(entry.lastEpoch).toISOString()),
      activeYears: entry.years.size,
      chapterCount: entry.seasons.size,
      primeHour: bestHour,
      mood: getHourMood(bestHour),
    };
  });

  const topTracks = [...trackMap.values()]
    .sort((a, b) => b.streams - a.streams || b.ms - a.ms)
    .slice(0, 250)
    .map((entry, index) => ({
      rank: index + 1,
      trackId: entry.trackId,
      track: entry.trackName,
      artist: entry.artistName,
      album: entry.albumName,
      streams: entry.streams,
      hours: toHours(entry.ms),
      firstSeen: toReadableDate(new Date(entry.firstEpoch).toISOString()),
      lastSeen: toReadableDate(new Date(entry.lastEpoch).toISOString()),
    }));

  const allOneStreamWonders = [...trackMap.values()]
    .filter((entry) => entry.streams === 1 && entry.ms >= 45_000)
    .sort((a, b) => b.ms - a.ms);

  const oneStreamWonderTotal = allOneStreamWonders.length;

  const oneStreamWonders = allOneStreamWonders
    .slice(0, 300)
    .map((entry, index) => ({
      rank: index + 1,
      trackId: entry.trackId,
      track: entry.trackName,
      artist: entry.artistName,
      album: entry.albumName,
      hours: toHours(entry.ms),
      playedAt: entry.firstDateLabel,
      memoryPrompt: `A one-off ${getHourMood(entry.firstHour ?? 12)} moment in your archive.`,
    }));

  const comebackArcs = buildComebackArcs(artistEventsByName)
    .slice(0, 100)
    .map((entry, index) => ({
      rank: index + 1,
      ...entry,
    }));

  const chapters = buildNarrativeChapters({
    seasonMap,
    timeline,
    comebackArcs,
  });

  const yearlyBreakdown = [...yearMap.values()]
    .sort((a, b) => a.year - b.year)
    .map((entry) => {
      const busiestMonth = [...entry.monthMs.entries()].sort((a, b) => b[1] - a[1])[0];
      const topYearArtists = [...entry.artistMap.entries()]
        .sort((a, b) => b[1].ms - a[1].ms)
        .slice(0, 5)
        .map(([artist, value]) => ({
          artist,
          hours: toHours(value.ms),
          streams: value.streams,
        }));

      const topYearTracks = [...entry.trackMap.values()]
        .sort((a, b) => b.streams - a.streams || b.ms - a.ms)
        .slice(0, 5)
        .map((track) => ({
          track: track.trackName,
          artist: track.artistName,
          streams: track.streams,
          hours: toHours(track.ms),
        }));

      const nightMs =
        entry.hourMs[22] +
        entry.hourMs[23] +
        entry.hourMs[0] +
        entry.hourMs[1] +
        entry.hourMs[2] +
        entry.hourMs[3] +
        entry.hourMs[4];

      let busiestMonthData = null;
      if (busiestMonth) {
        const [busiestYear, busiestMonthNum] = busiestMonth[0].split("-").map(Number);
        busiestMonthData = {
          monthKey: busiestMonth[0],
          label: monthNameFormatter.format(new Date(Date.UTC(busiestYear, busiestMonthNum - 1, 15))),
          hours: toHours(busiestMonth[1]),
        };
      }

      return {
        year: entry.year,
        hours: toHours(entry.ms),
        streams: entry.streams,
        trackHours: toHours(entry.typeMs.track),
        podcastHours: toHours(entry.typeMs.podcast),
        videoHours: toHours(entry.typeMs.video),
        audiobookHours: toHours(entry.typeMs.audiobook),
        uniqueArtists: entry.uniqueArtists.size,
        uniqueTracks: entry.uniqueTracks.size,
        skipRate: entry.streams === 0 ? 0 : Number((entry.skipCount / entry.streams).toFixed(4)),
        shuffleRate: entry.streams === 0 ? 0 : Number((entry.shuffleCount / entry.streams).toFixed(4)),
        offlineRate: entry.streams === 0 ? 0 : Number((entry.offlineCount / entry.streams).toFixed(4)),
        nightShare: entry.ms === 0 ? 0 : Number((nightMs / entry.ms).toFixed(4)),
        busiestMonth: busiestMonthData,
        topArtists: topYearArtists,
        topTracks: topYearTracks,
      };
    });

  const trackMs = typeCounters.track.qualifiedMs;
  const oneStreamWonderRatio = trackMap.size === 0 ? 0 : oneStreamWonderTotal / trackMap.size;
  const topArtistMs = [...artistMap.values()]
    .sort((a, b) => b.ms - a.ms)
    .slice(0, 20)
    .reduce((sum, artist) => sum + artist.ms, 0);
  const topArtistShare = trackMs === 0 ? 0 : topArtistMs / trackMs;

  const totalNightMs =
    listeningHourMs[22] +
    listeningHourMs[23] +
    listeningHourMs[0] +
    listeningHourMs[1] +
    listeningHourMs[2] +
    listeningHourMs[3] +
    listeningHourMs[4];

  const nightShare = totalQualifiedMs === 0 ? 0 : totalNightMs / totalQualifiedMs;
  const skipRate = totalQualifiedEvents === 0 ? 0 : skipCount / totalQualifiedEvents;
  const shuffleRate = totalQualifiedEvents === 0 ? 0 : shuffleCount / totalQualifiedEvents;
  const offlineRate = totalQualifiedEvents === 0 ? 0 : offlineCount / totalQualifiedEvents;
  const avgStreamMinutes = totalQualifiedEvents === 0 ? 0 : totalQualifiedMs / totalQualifiedEvents / 60_000;

  const longestStreakDays = longestStreakFromOrdinals([...activeDayOrdinals]);

  const topListeningHour = listeningHourMs.indexOf(Math.max(...listeningHourMs));

  const peakMonth = [...timeline].sort((a, b) => b.hours - a.hours)[0] || null;
  const quietMonth = [...timeline].sort((a, b) => a.hours - b.hours)[0] || null;

  const topCountries = [...countryMap.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([country, plays]) => ({ country, plays }));

  const summary = {
    generatedAt: new Date().toISOString(),
    timezone: ANALYSIS_TIMEZONE,
    sourceFiles: sourceFileStats,
    dateRange: {
      start: firstEvent.ts,
      end: lastEvent.ts,
      startLabel: dayLabelFormatter.format(new Date(firstEvent.epoch)),
      endLabel: dayLabelFormatter.format(new Date(lastEvent.epoch)),
      spanYears: Number(((lastEvent.epoch - firstEvent.epoch) / (365.25 * MS_PER_DAY)).toFixed(2)),
    },
    totals: {
      events: totalEvents,
      qualifiedEvents: totalQualifiedEvents,
      totalHours: toHours(totalMs),
      qualifiedHours: toHours(totalQualifiedMs),
      trackHours: toHours(typeCounters.track.qualifiedMs),
      podcastHours: toHours(typeCounters.podcast.qualifiedMs),
      videoHours: toHours(typeCounters.video.qualifiedMs),
      audiobookHours: toHours(typeCounters.audiobook.qualifiedMs),
      uniqueArtists: artistMap.size,
      uniqueTracks: trackMap.size,
      oneStreamWonders: oneStreamWonderTotal,
      activeDays: activeDayOrdinals.size,
      longestStreakDays,
    },
    behavior: {
      skipRate: Number(skipRate.toFixed(4)),
      shuffleRate: Number(shuffleRate.toFixed(4)),
      offlineRate: Number(offlineRate.toFixed(4)),
      nightShare: Number(nightShare.toFixed(4)),
      topListeningHour,
      avgStreamMinutes: Number(avgStreamMinutes.toFixed(2)),
    },
    milestones: {
      peakMonth,
      quietMonth,
      topCountries,
    },
  };

  const personality = buildPersonality({
    totalQualifiedStreams: totalQualifiedEvents,
    totalTrackMs: trackMs,
    skipRate,
    shuffleRate,
    nightShare,
    oneStreamWonderRatio,
    topArtistShare,
    avgStreamMinutes,
  });

  const manifest = {
    generatedAt: summary.generatedAt,
    timezone: ANALYSIS_TIMEZONE,
    files: [
      "summary.json",
      "timeline_monthly.json",
      "top_artists.json",
      "top_tracks.json",
      "yearly_breakdown.json",
      "chapters.json",
      "one_stream_wonders.json",
      "comeback_arcs.json",
      "personality.json",
    ],
  };

  const output = {
    "summary.json": summary,
    "timeline_monthly.json": timeline,
    "top_artists.json": topArtists,
    "top_tracks.json": topTracks,
    "yearly_breakdown.json": yearlyBreakdown,
    "chapters.json": chapters,
    "one_stream_wonders.json": oneStreamWonders,
    "comeback_arcs.json": comebackArcs,
    "personality.json": personality,
    "manifest.json": manifest,
  };

  for (const [fileName, payload] of Object.entries(output)) {
    const destination = path.join(OUTPUT_DIR, fileName);
    fs.writeFileSync(destination, JSON.stringify(payload, null, 2));
  }

  console.log("Stream Story preprocessing complete.");
  console.log(`Timezone: ${ANALYSIS_TIMEZONE}`);
  console.log(`Events: ${summary.totals.events} total, ${summary.totals.qualifiedEvents} qualified`);
  console.log(`Track hours: ${summary.totals.trackHours}`);
  console.log(`Artists: ${summary.totals.uniqueArtists}, tracks: ${summary.totals.uniqueTracks}`);
  console.log(`Output folder: ${OUTPUT_DIR}`);
}

try {
  main();
} catch (error) {
  console.error("Preprocessing failed.");
  console.error(error);
  process.exitCode = 1;
}
