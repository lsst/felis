// The content of the page: sorting, search and CSV export for its tables,
// with DataTables; jumps to a table row; the pane's sizes; the Top button; and
// a schema's relationship diagram, drawn by Mermaid.
(function () {
  // Tables, with DataTables

  // The table matching selector, if the page has one and DataTables, which
  // the page loads from a CDN, has loaded; otherwise null, and the table
  // stays a plain table.
  function dataTableElement(selector) {
    var table = document.querySelector(selector);
    if (!table || typeof window.DataTable === "undefined") {
      return null;
    }
    return table;
  }

  // Make a DataTable of a table with the options common to every table: all
  // rows shown, in the order of the page.
  function makeDataTable(table, options) {
    var settings = { paging: false, order: [] };
    Object.assign(settings, options);
    return new window.DataTable(table, settings);
  }

  // Sorting, search and CSV export for the columns table. Kept for jumps to a
  // row hidden by the table's search.
  var columnsTable = null;

  function initColumnsTable() {
    var table = dataTableElement(".columns-table");
    if (!table) {
      return;
    }
    columnsTable = makeDataTable(table, {
      // The row count is centred in the top row by style.css; DataTables has
      // no centre position.
      layout: { topStart: ["buttons", "info"], topEnd: "search", bottomStart: null, bottomEnd: null },
      infoCallback: rowCountText,
      buttons: [
        {
          extend: "csvHtml5",
          filename: table.dataset.exportName,
          // Export the name cells' data-order value, without the primary key
          // tag.
          exportOptions: { orthogonal: "sort" },
        },
      ],
    });
  }

  // The columns table's row count. Every row is always shown, so the count is
  // just the number of rows, or "N of M rows" while searching.
  function rowCountText(settings, start, end, max, total) {
    var count = total.toLocaleString("en-US");
    var noun = max === 1 ? " row" : " rows";
    if (total !== max) {
      count += " of " + max.toLocaleString("en-US");
    }
    return count + noun;
  }

  // Sorting for the table of schemas on the home page or of tables on a
  // schema page: no search, row count or export.
  function initSummaryTable() {
    var table = dataTableElement(".summary-table");
    if (!table) {
      return;
    }
    makeDataTable(table, {
      searching: false,
      info: false,
      layout: { topStart: null, topEnd: null, bottomStart: null, bottomEnd: null },
    });
  }

  // Jumps to a table row

  // Jumping to a column, index or constraint row should only scroll the
  // content pane vertically: keep it scrolled fully left, even if it was
  // scrolled right before the jump.
  function initJumpScrolling() {
    if (!document.querySelector(".content-body")) {
      return;
    }
    window.addEventListener("hashchange", showJumpTarget);
    window.addEventListener("load", onPageLoad);
  }

  function scrollPaneFullyLeft() {
    document.querySelector(".content-body").scrollLeft = 0;
  }

  // A search hides non-matching columns, so a jump to one of them clears the
  // search to bring the row back before scrolling to it.
  function showJumpTarget() {
    var id = decodeURIComponent(window.location.hash.slice(1));
    if (id && !document.getElementById(id) && columnsTable && columnsTable.search()) {
      columnsTable.search("").draw();
      // Navigate to the fragment again now that its row exists, so it is
      // scrolled to and highlighted as the target (:target).
      if (document.getElementById(id)) {
        window.location.replace(window.location.href);
      }
    }
    scrollPaneFullyLeft();
  }

  function onPageLoad() {
    if (window.location.hash) {
      scrollPaneFullyLeft();
    }
  }

  // Content pane sizes

  // Content pinned while the columns table scrolls sideways is sized to the
  // pane's visible content width, which CSS can't measure because it excludes
  // the pane's vertical scrollbar. Keep --pane-width up to date. Jumps to a
  // row of the columns table should stop just below its pinned header row, so
  // keep --columns-header-height set to that row's height too.
  function initPaneWidth() {
    var pane = document.querySelector(".content-body");
    if (!pane || typeof window.ResizeObserver === "undefined") {
      return;
    }
    new window.ResizeObserver(updatePaneWidth).observe(pane);
    updatePaneWidth();

    var header = document.querySelector(".columns-table thead");
    if (header) {
      new window.ResizeObserver(updateColumnsHeaderHeight).observe(header);
    }
  }

  function updatePaneWidth() {
    var pane = document.querySelector(".content-body");
    var style = window.getComputedStyle(pane);
    var width = pane.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
    pane.style.setProperty("--pane-width", width + "px");
  }

  function updateColumnsHeaderHeight() {
    var pane = document.querySelector(".content-body");
    var header = document.querySelector(".columns-table thead");
    pane.style.setProperty("--columns-header-height", header.offsetHeight + "px");
  }

  // Top button

  // The "Top" button in the header scrolls the content pane back to its start.
  function initScrollToTop() {
    var button = document.getElementById("scroll-to-top");
    if (!button || !document.querySelector(".content-body")) {
      return;
    }
    button.addEventListener("click", onScrollToTopClick);
  }

  function onScrollToTopClick() {
    var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var pane = document.querySelector(".content-body");
    pane.scrollTo({ top: 0, left: 0, behavior: reduceMotion ? "auto" : "smooth" });
  }

  // Relationship diagrams

  // Relationship diagrams on schema pages (when enabled in the site
  // configuration): Mermaid, loaded from a CDN only on pages that have one,
  // draws the diagram text into an SVG fitted to the page width. A dynamic
  // import works when the site is opened from disk, unlike a module script,
  // but can't carry an integrity hash as the DataTables script tags do, so
  // the file is trusted as the CDN serves it.
  var MERMAID_URL = "https://cdn.jsdelivr.net/npm/mermaid@12.2.0/dist/mermaid.esm.min.mjs";

  function initDiagrams() {
    if (!document.querySelector(".schema-diagram")) {
      return;
    }
    import(MERMAID_URL).then(drawDiagrams).then(showFullSizeButtons).catch(keepDiagramText);
  }

  function drawDiagrams(module) {
    var mermaid = module.default;
    mermaid.initialize({
      startOnLoad: false,
      theme: "neutral",
      securityLevel: "strict",
      // SVG text labels rather than HTML ones (in foreignObject), which
      // Safari draws at full size when the diagram is scaled down.
      htmlLabels: false,
    });
    return mermaid.run({ nodes: document.querySelectorAll(".schema-diagram pre.mermaid") });
  }

  // Without Mermaid (e.g. offline) the diagram text stays visible.
  function keepDiagramText() {}

  // Show each drawn diagram's "Open full size" button.
  function showFullSizeButtons() {
    for (var diagram of document.querySelectorAll(".schema-diagram")) {
      var actions = diagram.querySelector(".schema-diagram-actions");
      if (diagram.querySelector("svg") && actions) {
        actions.hidden = false;
        actions.querySelector("button").addEventListener("click", onFullSizeClick);
      }
    }
  }

  // "Open full size" shows the drawn diagram on its own in a new tab.
  function onFullSizeClick(event) {
    var svg = event.currentTarget.closest(".schema-diagram").querySelector("svg");
    var copy = svg.cloneNode(true);
    copy.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    copy.style.maxWidth = "none";
    var url = URL.createObjectURL(new Blob([copy.outerHTML], { type: "image/svg+xml" }));
    window.open(url, "_blank");
    // Free the copy once the new tab has had time to load it.
    setTimeout(revokeUrl, 60000, url);
  }

  function revokeUrl(url) {
    URL.revokeObjectURL(url);
  }

  function init() {
    initColumnsTable();
    initSummaryTable();
    initJumpScrolling();
    initScrollToTop();
    initPaneWidth();
    initDiagrams();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
