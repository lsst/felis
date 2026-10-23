// The sidebar: the tree of schemas and tables, built from the navigation
// data (window.FELIS_NAV, defined by nav-data.js), its resizer and its
// collapse button. The rest of the page's behavior is in page.js, and the
// search in search.js.
(function () {
  // Sidebar resizer

  // Dragging the resizer sets the sidebar's width as --sidebar-width on the
  // body. The stylesheet keeps it between the minimum and maximum widths and
  // sizes the sidebar, the resizer and the content from it, so its rules for
  // the collapsed sidebar still apply after a resize. Pointer events cover a
  // mouse, touch and a pen; capturing the pointer sends its moves to the
  // resizer while dragging, wherever it goes.
  function initSidebarResize() {
    var resizer = document.getElementById("sidebar-resizer");
    if (!resizer) {
      return;
    }
    resizer.addEventListener("pointerdown", onResizerPointerDown);
    resizer.addEventListener("pointerup", onResizerPointerUp);
    resizer.addEventListener("pointercancel", onResizerPointerUp);
  }

  function onResizerPointerDown(event) {
    var resizer = event.currentTarget;
    resizer.addEventListener("pointermove", onResizerPointerMove);
    document.body.style.userSelect = "none";
    resizer.setPointerCapture(event.pointerId);
  }

  function onResizerPointerMove(event) {
    document.body.style.setProperty("--sidebar-width", String(event.clientX) + "px");
  }

  function onResizerPointerUp(event) {
    event.currentTarget.removeEventListener("pointermove", onResizerPointerMove);
    document.body.style.userSelect = "";
  }

  // Sidebar collapse button

  function setCollapsed(collapsed) {
    document.body.classList.toggle("sidebar-collapsed", collapsed);
    var button = document.getElementById("sidebar-toggle");
    if (button) {
      button.setAttribute("aria-expanded", String(!collapsed));
    }
  }

  function initSidebarToggle() {
    var button = document.getElementById("sidebar-toggle");
    if (!button) {
      return;
    }
    button.addEventListener("click", onSidebarToggleClick);
    setCollapsed(document.body.classList.contains("sidebar-collapsed"));
  }

  // The choice is kept for the browser tab's session, so the sidebar stays
  // collapsed or expanded from page to page; base.html.j2 applies it before
  // the page is drawn.
  function onSidebarToggleClick() {
    var collapsed = !document.body.classList.contains("sidebar-collapsed");
    setCollapsed(collapsed);
    try {
      sessionStorage.setItem("felis-sidebar-collapsed", String(collapsed));
    } catch (e) {
      // Storage is blocked; the choice applies to this page only.
    }
  }

  // Sidebar tree

  // Build a link to a page in the site, optionally to an anchor on that page.
  // The description, if any, becomes the hover text.
  function navLink(root, path, anchor, text, description) {
    var link = document.createElement("a");
    link.href = root + "/" + path + (anchor ? "#" + anchor : "");
    link.textContent = text;
    if (description) {
      link.title = description;
    }
    return link;
  }

  // Build a collapsible tree node whose summary is the given link.
  function treeNode(link) {
    var details = document.createElement("details");
    details.className = "tree-node";
    var summary = document.createElement("summary");
    summary.appendChild(link);
    details.appendChild(summary);
    return details;
  }

  function listItem(child) {
    var item = document.createElement("li");
    item.appendChild(child);
    return item;
  }

  // Build one collapsible section (Columns, Indexes or Constraints) of a
  // table node, showing how many entries it has. The section starts closed,
  // and its list is built the first time it is opened.
  function treeSection(root, table, kind, label, entries) {
    var heading = navLink(root, table.path, table.anchor + "-" + kind, label);
    heading.className = "tree-section-heading";
    var section = treeNode(heading);
    section.classList.add("tree-section", "tree-" + kind);
    var count = document.createElement("span");
    count.className = "tree-section-count";
    count.textContent = "(" + entries.length + ")";
    section.querySelector("summary").appendChild(count);
    section.addEventListener(
      "toggle",
      function () {
        fillTreeSection(section, root, table, entries);
      },
      { once: true }
    );
    return section;
  }

  // Add a section's list of entries, each linking to its row on the table's
  // page.
  function fillTreeSection(section, root, table, entries) {
    var list = document.createElement("ul");
    for (var entry of entries) {
      list.appendChild(listItem(navLink(root, table.path, entry.anchor, entry.name, entry.description)));
    }
    section.appendChild(list);
  }

  function fillTableSections(tableNode, root, table) {
    tableNode.appendChild(treeSection(root, table, "columns", "Columns", table.columns));
    if (table.indexes.length) {
      tableNode.appendChild(treeSection(root, table, "indexes", "Indexes", table.indexes));
    }
    if (table.constraints.length) {
      tableNode.appendChild(treeSection(root, table, "constraints", "Constraints", table.constraints));
    }
  }

  // Build a table's node. The current page's table starts open with its
  // sections; any other table's sections are added the first time it is
  // opened.
  function tableTreeNode(root, table, isCurrent) {
    var tableNode = treeNode(navLink(root, table.path, "", table.name, table.description));
    if (isCurrent) {
      fillTableSections(tableNode, root, table);
      tableNode.open = true;
    } else {
      tableNode.addEventListener(
        "toggle",
        function () {
          fillTableSections(tableNode, root, table);
        },
        { once: true }
      );
    }
    return tableNode;
  }

  // Mark the sidebar entry for the current page (styled by style.css and
  // announced by screen readers), and scroll the sidebar to it if it's out
  // of view. Only the sidebar scrolls; the content pane is separate.
  function markCurrent(link) {
    if (!link) {
      return;
    }
    link.setAttribute("aria-current", "page");

    var sidebar = document.querySelector(".sidebar");
    var header = document.querySelector(".sidebar-header");
    if (!sidebar) {
      return;
    }
    var view = sidebar.getBoundingClientRect();
    var top = view.top + (header ? header.offsetHeight : 0);
    var entry = link.getBoundingClientRect();
    if (entry.top < top || entry.bottom > view.bottom) {
      link.scrollIntoView({ block: "center" });
    }
  }

  // Build the sidebar tree from the shared navigation data (window.FELIS_NAV,
  // defined by nav-data.js). Only schemas and tables are built up front; a
  // table's sections are added the first time it is expanded.
  function initSidebarTree() {
    var container = document.getElementById("sidebar-tree");
    var nav = window.FELIS_NAV;
    if (!container || !nav) {
      return;
    }

    var page = document.body.dataset;
    var root = page.siteRoot || ".";
    var schemaList = document.createElement("ul");
    schemaList.className = "tree";
    var currentLink = null;

    for (var schema of nav.schemas) {
      var schemaNode = treeNode(navLink(root, schema.path, "", schema.name, schema.description));
      if (schema.path === page.schemaPath && !page.tablePath) {
        currentLink = schemaNode.querySelector("summary > a");
      }
      var tableList = document.createElement("ul");
      for (var table of schema.tables) {
        var tableNode = tableTreeNode(root, table, table.path === page.tablePath);
        if (table.path === page.tablePath) {
          currentLink = tableNode.querySelector("summary > a");
        }
        tableList.appendChild(listItem(tableNode));
      }
      schemaNode.appendChild(tableList);
      schemaNode.open = schema.path === page.schemaPath;
      schemaList.appendChild(listItem(schemaNode));
    }

    container.appendChild(schemaList);
    markCurrent(currentLink);
    container.addEventListener("click", onTreeClick);
  }

  // Clicking a schema or table link navigates without toggling its node,
  // preventing the flash of the node collapsing on click. Other clicks, such
  // as Ctrl-click to open a new tab, are left to the browser.
  function onTreeClick(event) {
    var link = event.target.closest("summary > a");
    var plainClick =
      event.button === 0 && !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey;
    if (link && plainClick) {
      event.preventDefault();
      window.location.href = link.href;
    }
  }

  function init() {
    initSidebarTree();
    initSidebarToggle();
    initSidebarResize();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
