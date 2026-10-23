// Pop-up search over the names and descriptions of every schema, table, column,
// index and constraint, and the UCDs of columns, from the sidebar's navigation
// data (window.FELIS_NAV, defined by nav-data.js). It opens from the search
// button in the page header, next to the Top button, or with Ctrl+K (Cmd+K on
// macOS).
//
// Names are matched directly; MiniSearch, loaded from a CDN the first time the
// search opens, adds full-text search of the descriptions and UCDs. Without it
// (e.g. offline), only names are searched.
(function () {
  // Loaded with a dynamic import, which can't carry an integrity hash as the
  // DataTables script tags do, so the file is trusted as the CDN serves it.
  var MINISEARCH_URL = "https://cdn.jsdelivr.net/npm/minisearch@7.2.0/dist/es/index.min.js";
  var PAGE_SIZE = 50; // Results shown at a time; more load on scrolling.
  var KINDS = [
    { kind: "schema", label: "Schemas", one: "schema" },
    { kind: "table", label: "Tables", one: "table" },
    { kind: "column", label: "Columns", one: "column" },
    { kind: "index", label: "Indexes", one: "index" },
    { kind: "constraint", label: "Constraints", one: "constraint" },
  ];
  var KIND_ORDER = { schema: 0, table: 1, column: 2, index: 3, constraint: 4 };
  // The tabs: All, then one per kind.
  var TABS = [{ kind: "all", label: "Results", one: "result", tab: "All" }].concat(KINDS);

  var dialog, input, tabs, results, status, openButton;
  var activeTab = "all"; // The tab shown: "all", or a kind of results.
  var chosenTab = null; // The tab chosen, if any, since opening.
  var tabButtons = {};
  var entries = null;
  var fullText = null; // The MiniSearch index, once loaded.
  var fullTextFailed = false;
  var shown = []; // The result links shown, in order.
  var pending = null; // The current results, for loading more of them.
  var selected = -1;
  var timer = null;

  // Searchable entries

  // Flatten the navigation data into one entry per searchable item, with
  // its link (from the site root) and where it is ("dp1 › Object").
  function buildEntries() {
    var nav = window.FELIS_NAV;
    var root = document.body.dataset.siteRoot || ".";
    var list = [];
    if (!nav) {
      return list;
    }
    for (var schema of nav.schemas) {
      list.push(searchEntry(list.length, "schema", schema, root + "/" + schema.path, ""));
      for (var table of schema.tables) {
        var page = root + "/" + table.path;
        var location = schema.name + " › " + table.name;
        list.push(searchEntry(list.length, "table", table, page, schema.name));
        // A table's columns, indexes and constraints are rows on its page.
        var rows = [
          ["column", table.columns],
          ["index", table.indexes],
          ["constraint", table.constraints],
        ];
        for (var [kind, items] of rows) {
          for (var item of items) {
            list.push(searchEntry(list.length, kind, item, page + "#" + item.anchor, location));
          }
        }
      }
    }
    return list;
  }

  // An entry of the search, for an item of the navigation data. Its ID is its
  // position in the list of entries, which MiniSearch reports matches by.
  function searchEntry(id, kind, item, href, location) {
    return {
      id: id,
      kind: kind,
      name: item.name,
      description: item.description || "",
      ucd: item.ucd || "",
      href: href,
      location: location,
    };
  }

  // Full-text search

  // Load MiniSearch and index the descriptions and UCDs (and names, so a word
  // inside a name also matches). Searches made while it loads use names only,
  // and are repeated once it's ready.
  function loadFullText() {
    import(MINISEARCH_URL).then(indexFullText).catch(onFullTextFailed);
  }

  function indexFullText(module) {
    var MiniSearch = module.default;
    var index = new MiniSearch({
      fields: ["name", "description", "ucd"],
      tokenize: words,
    });
    index.addAll(entries);
    fullText = index;
    update();
  }

  function onFullTextFailed() {
    fullTextFailed = true;
    update();
  }

  // Split text into words for searching: at anything other than letters,
  // digits and underscores, so names such as "cdb_latiss" or "day_obs" stay
  // whole words, while UCDs such as "phot.mag;em.opt" split at the dots.
  function words(text) {
    return text.split(/[^\p{L}\p{N}_]+/u).filter(Boolean);
  }

  // Searching

  // How well a name matches the query: 3 for the whole name, 2 for its
  // start, 1 anywhere in it, 0 not at all.
  function nameRank(name, query) {
    var lower = name.toLowerCase();
    if (lower === query) {
      return 3;
    }
    if (lower.indexOf(query) === 0) {
      return 2;
    }
    return lower.indexOf(query) >= 0 ? 1 : 0;
  }

  // Whether a MiniSearch result matched the entry's UCD, so the result shows
  // it.
  function matchedUcd(result) {
    for (var term in result.match) {
      if (result.match[term].indexOf("ucd") >= 0) {
        return true;
      }
    }
    return false;
  }

  // Order matches: name matches first (whole name, then start, then
  // anywhere), then full-text matches by their score, then by kind and name.
  function compareMatches(a, b) {
    return (
      b.rank - a.rank ||
      b.score - a.score ||
      KIND_ORDER[a.entry.kind] - KIND_ORDER[b.entry.kind] ||
      a.entry.name.localeCompare(b.entry.name)
    );
  }

  // Find the entries matching the query. Each match has its entry, the rank
  // of its name match (see nameRank), its full-text score, and whether its
  // UCD matched.
  function search(text) {
    var query = text.trim().toLowerCase();
    // The matches in the order found (name matches, then full-text ones),
    // which the sort keeps for ties, and by entry ID.
    var found = [];
    var matches = {};

    for (var entry of entries) {
      var rank = nameRank(entry.name, query);
      if (rank) {
        matches[entry.id] = { entry: entry, rank: rank, score: 0, ucd: false };
        found.push(matches[entry.id]);
      }
    }

    if (fullText && query.length >= 2) {
      var fullTextResults = fullText.search(query, {
        boost: { name: 3, ucd: 2 },
        prefix: true,
        combineWith: "AND",
      });
      for (var result of fullTextResults) {
        var match = matches[result.id];
        if (!match) {
          match = { entry: entries[result.id], rank: 0, score: 0, ucd: false };
          matches[result.id] = match;
          found.push(match);
        }
        match.score = result.score;
        match.ucd = matchedUcd(result);
      }
    }

    found.sort(compareMatches);
    return { query: query, found: found };
  }

  // Highlighting

  // Append text to an element, wrapping the parts matching any of the
  // patterns in <mark>. Text is added as text, never as HTML.
  function appendHighlighted(element, text, pattern) {
    if (!pattern) {
      element.appendChild(document.createTextNode(text));
      return;
    }
    var last = 0;
    for (var match of text.matchAll(pattern)) {
      if (!match[0]) {
        continue;
      }
      element.appendChild(document.createTextNode(text.slice(last, match.index)));
      var mark = document.createElement("mark");
      mark.textContent = match[0];
      element.appendChild(mark);
      last = match.index + match[0].length;
    }
    element.appendChild(document.createTextNode(text.slice(last)));
  }

  function escapeRegExp(text) {
    return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  // The part of a description to show: from a little before its first
  // highlighted word, so the match is in view.
  function excerpt(description, pattern) {
    if (!pattern) {
      return description;
    }
    pattern.lastIndex = 0;
    var match = pattern.exec(description);
    pattern.lastIndex = 0;
    if (!match || match.index < 40) {
      return description;
    }
    var start = description.lastIndexOf(" ", match.index - 30);
    return "…" + description.slice(start + 1);
  }

  function compareLongestFirst(a, b) {
    return b.length - a.length;
  }

  // The patterns of the text to highlight in results, built once per search.
  // Highlight only the text searched for, not the rest of the words it
  // matched: in names, the query or any of its words anywhere, such as
  // "flux" in "z_psfFlux"; in descriptions and UCDs, the query's words at the
  // start of words. Longer alternatives come first, so the longest match is
  // highlighted.
  function highlightPatterns(query) {
    var queryWords = words(query);
    var parts = [query].concat(queryWords).sort(compareLongestFirst).map(escapeRegExp);
    var alternatives = queryWords.map(escapeRegExp).join("|");
    return {
      name: new RegExp(parts.join("|"), "gi"),
      // A word start is a letter, digit or underscore that doesn't follow
      // one, in any script; \b only knows ASCII letters.
      words: alternatives
        ? new RegExp("(?<![\\p{L}\\p{N}_])(?:" + alternatives + ")", "giu")
        : null,
    };
  }

  // Results

  function renderResult(match, patterns, number) {
    var entry = match.entry;
    var link = document.createElement("a");
    link.className = "search-result";
    link.href = entry.href;
    link.id = "search-result-" + number;
    link.dataset.index = String(number);
    link.setAttribute("role", "option");
    link.setAttribute("aria-selected", "false");
    link.tabIndex = -1;

    var heading = document.createElement("span");
    heading.className = "search-result-heading";
    var name = document.createElement("span");
    name.className = "search-result-name";
    appendHighlighted(name, entry.name, patterns.name);
    heading.appendChild(name);
    // A column's UCD, in brackets after its name, when the search matched it.
    if (match.ucd && entry.ucd) {
      var ucd = document.createElement("span");
      ucd.className = "search-result-ucd";
      ucd.appendChild(document.createTextNode("["));
      appendHighlighted(ucd, entry.ucd, patterns.words);
      ucd.appendChild(document.createTextNode("]"));
      heading.appendChild(ucd);
    }
    if (entry.location) {
      var location = document.createElement("span");
      location.className = "search-result-location";
      location.textContent = entry.location;
      heading.appendChild(location);
    }
    link.appendChild(heading);

    if (entry.description) {
      var description = document.createElement("span");
      description.className = "search-result-description";
      appendHighlighted(description, excerpt(entry.description, patterns.words), patterns.words);
      link.appendChild(description);
    }

    link.addEventListener("mousemove", onResultHover);
    link.addEventListener("click", onResultClick);
    return link;
  }

  // Hovering selects the result without scrolling, which would move the list
  // under the pointer.
  function onResultHover(event) {
    var index = Number(event.currentTarget.dataset.index);
    if (index !== selected) {
      select(index, false);
    }
  }

  function onResultClick() {
    dialog.close();
  }

  // The status line at the bottom: the number of results, announced by
  // screen readers. Hidden while a message is shown instead of results.
  function setStatus(text) {
    status.textContent = text;
    status.hidden = !text;
  }

  // A message in the middle of the results area, for an empty search or no
  // results. It is also put in the status line for screen readers.
  function showMessage(text) {
    var message = document.createElement("p");
    message.className = "search-message";
    message.textContent = text;
    results.appendChild(message);
    status.textContent = text;
    status.hidden = true;
  }

  // Tabs

  // The tabs above the results, one per kind, each with its number of
  // results. Tabs with none are disabled.
  function buildTabs() {
    for (var group of TABS) {
      var tab = document.createElement("button");
      tab.type = "button";
      tab.className = "search-tab";
      tab.id = "search-tab-" + group.kind;
      tab.dataset.kind = group.kind;
      tab.setAttribute("role", "tab");
      tab.setAttribute("aria-controls", "search-results");
      tab.tabIndex = -1;
      tab.appendChild(document.createTextNode(group.tab || group.label));
      var count = document.createElement("span");
      count.className = "search-tab-count";
      tab.appendChild(count);
      tab.addEventListener("click", onTabClick);
      tabs.appendChild(tab);
      tabButtons[group.kind] = tab;
    }
  }

  function onTabClick(event) {
    chooseTab(event.currentTarget.dataset.kind);
    input.focus();
  }

  function chooseTab(kind) {
    chosenTab = kind;
    update();
  }

  // The tab of a kind of results ("all" for All).
  function tabOf(kind) {
    for (var group of TABS) {
      if (group.kind === kind) {
        return group;
      }
    }
    return null;
  }

  // Move to the next or previous tab that has results (Ctrl+→ and Ctrl+←).
  function stepTab(step) {
    var index = TABS.indexOf(tabOf(activeTab));
    for (var i = index + step; i >= 0 && i < TABS.length; i += step) {
      if (!tabButtons[TABS[i].kind].disabled) {
        chooseTab(TABS[i].kind);
        return;
      }
    }
  }

  function updateTabs(counts) {
    for (var group of TABS) {
      var tab = tabButtons[group.kind];
      var count = counts ? counts[group.kind] : null;
      var countText = count === null ? "" : count.toLocaleString("en-US");
      tab.querySelector(".search-tab-count").textContent = countText;
      tab.disabled = group.kind !== "all" && count === 0;
      tab.setAttribute("aria-selected", String(group.kind === activeTab));
    }
  }

  // Showing the results

  // The number of matches of each kind, and of all kinds as "all".
  function countByKind(found) {
    var counts = { all: found.length };
    for (var group of KINDS) {
      counts[group.kind] = 0;
    }
    for (var match of found) {
      counts[match.entry.kind] += 1;
    }
    return counts;
  }

  function compareByKind(a, b) {
    return KIND_ORDER[a.entry.kind] - KIND_ORDER[b.entry.kind];
  }

  // The matches shown on the active tab: on All, every match, grouped by kind
  // (the sort keeps their order within each kind); on another tab, the
  // matches of its kind.
  function matchesOnTab(found) {
    if (activeTab === "all") {
      return found.slice().sort(compareByKind);
    }
    var onTab = [];
    for (var match of found) {
      if (match.entry.kind === activeTab) {
        onTab.push(match);
      }
    }
    return onTab;
  }

  // Show the results for the current query on the tab chosen (or All, if
  // none is chosen or it has no results).
  function update() {
    if (!dialog || !dialog.open) {
      return;
    }
    var text = input.value;
    results.textContent = "";
    shown = [];
    pending = null;
    selected = -1;
    input.removeAttribute("aria-activedescendant");

    if (!text.trim()) {
      activeTab = "all";
      updateTabs(null);
      showMessage(
        "Search schemas, tables, columns, indexes and constraints by name or description, " +
          "and columns by UCD. Other fields, such as datatypes and units, aren't searched. " +
          "Press Ctrl+← or Ctrl+→ to switch between the result tabs."
      );
      return;
    }

    var outcome = search(text);
    var counts = countByKind(outcome.found);
    activeTab = chosenTab && counts[chosenTab] ? chosenTab : "all";
    updateTabs(counts);

    var note = fullTextFailed ? " Searching names only: full-text search couldn't load." : "";
    if (!outcome.found.length) {
      showMessage("Nothing matches “" + text.trim() + "”." + note);
      return;
    }

    var found = matchesOnTab(outcome.found);
    var list = document.createElement("div");
    list.setAttribute("role", "group");
    list.setAttribute("aria-labelledby", "search-tab-" + activeTab);
    results.appendChild(list);
    results.scrollTop = 0;
    pending = {
      found: found,
      patterns: highlightPatterns(outcome.query),
      list: list,
      kind: null, // The kind of the last result shown, on All.
    };
    loadMore();

    var total = found.length;
    var tab = tabOf(activeTab);
    var noun = total === 1 ? tab.one : tab.label.toLowerCase();
    setStatus(total.toLocaleString("en-US") + " " + noun + "." + note);
    select(0);
  }

  // Show the next page of the current results, if there are more. Called
  // when the results are scrolled near their end, or ↓ reaches the last one.
  function loadMore() {
    if (!pending || shown.length >= pending.found.length) {
      return false;
    }
    for (var match of pending.found.slice(shown.length, shown.length + PAGE_SIZE)) {
      // On All, a heading starts each kind of result.
      if (activeTab === "all" && match.entry.kind !== pending.kind) {
        pending.kind = match.entry.kind;
        var heading = document.createElement("div");
        heading.className = "search-group-heading";
        heading.setAttribute("role", "presentation");
        heading.textContent = KINDS[KIND_ORDER[match.entry.kind]].label;
        pending.list.appendChild(heading);
      }
      var link = renderResult(match, pending.patterns, shown.length);
      shown.push(link);
      pending.list.appendChild(link);
    }
    return true;
  }

  // Select a result, scrolling it into view unless scroll is false.
  function select(index, scroll) {
    if (index < 0 || index >= shown.length) {
      return;
    }
    if (selected >= 0 && shown[selected]) {
      shown[selected].setAttribute("aria-selected", "false");
    }
    selected = index;
    shown[selected].setAttribute("aria-selected", "true");
    if (scroll !== false) {
      shown[selected].scrollIntoView({ block: "nearest" });
    }
    input.setAttribute("aria-activedescendant", shown[selected].id);
  }

  // Opening, closing and keys

  function open() {
    if (dialog.open) {
      input.select();
      return;
    }
    if (!entries) {
      entries = buildEntries();
      loadFullText();
    }
    chosenTab = null;
    dialog.showModal();
    input.select();
    update();
  }

  // Open the result: a column, index or constraint on this page is jumped
  // to (page.js clears the columns table's search if it hides the row).
  function go(link) {
    dialog.close();
    window.location.href = link.href;
  }

  function onInputKey(event) {
    var tabKey = event.key === "ArrowRight" || event.key === "ArrowLeft";
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (selected === shown.length - 1) {
        loadMore();
      }
      select(Math.min(selected + 1, shown.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      select(Math.max(selected - 1, 0));
    } else if ((event.ctrlKey || event.metaKey) && tabKey) {
      event.preventDefault();
      stepTab(event.key === "ArrowRight" ? 1 : -1);
    } else if (event.key === "Enter" && selected >= 0) {
      event.preventDefault();
      go(shown[selected]);
    }
  }

  // Ctrl+K (Cmd+K) opens the search, even while typing in a field.
  function onDocumentKey(event) {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      open();
    }
  }

  // Search after a short pause in typing, not on every key.
  function onInput() {
    window.clearTimeout(timer);
    timer = window.setTimeout(update, 100);
  }

  // Load more results when the list is scrolled near its end.
  function onResultsScroll() {
    if (results.scrollTop + results.clientHeight >= results.scrollHeight - 300) {
      loadMore();
    }
  }

  function onCloseClick() {
    dialog.close();
  }

  // A click on the backdrop (outside the panel) closes the search.
  function onDialogClick(event) {
    if (event.target === dialog) {
      dialog.close();
    }
  }

  function onDialogClose() {
    openButton.focus();
  }

  function init() {
    dialog = document.getElementById("search-dialog");
    openButton = document.getElementById("search-open");
    if (!dialog || !openButton || typeof dialog.showModal !== "function") {
      return;
    }
    input = document.getElementById("search-input");
    tabs = document.getElementById("search-tabs");
    results = document.getElementById("search-results");
    status = document.getElementById("search-status");
    buildTabs();

    openButton.addEventListener("click", open);
    document.getElementById("search-close").addEventListener("click", onCloseClick);
    dialog.addEventListener("click", onDialogClick);
    dialog.addEventListener("close", onDialogClose);
    input.addEventListener("input", onInput);
    input.addEventListener("keydown", onInputKey);
    results.addEventListener("scroll", onResultsScroll);
    document.addEventListener("keydown", onDocumentKey);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
