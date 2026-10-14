(function () {
  function initSidebarResize() {
    var sidebar = document.querySelector(".sidebar");
    var resizer = document.getElementById("sidebar-resizer");
    var layout = document.querySelector(".layout");
    if (!sidebar || !resizer) {
      return;
    }

    var isDragging = false;

    function onMouseMove(event) {
      if (!isDragging) {
        return;
      }
      var minWidth = 220;
      var maxWidth = Math.floor(window.innerWidth * 0.33);
      var newWidth = Math.min(Math.max(event.clientX, minWidth), maxWidth);
      sidebar.style.width = String(newWidth) + "px";
      resizer.style.left = String(newWidth) + "px";
      if (layout) {
        layout.style.marginLeft = String(newWidth) + "px";
      }
    }

    function onMouseUp() {
      isDragging = false;
      document.body.style.userSelect = "";
    }

    resizer.addEventListener("mousedown", function () {
      if (window.innerWidth <= 900) {
        return;
      }
      isDragging = true;
      document.body.style.userSelect = "none";
    });

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  }

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

    button.addEventListener("click", function () {
      var collapsed = !document.body.classList.contains("sidebar-collapsed");
      setCollapsed(collapsed);
    });

    setCollapsed(false);
  }

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
    section.addEventListener("toggle", function fill() {
      section.removeEventListener("toggle", fill);
      var list = document.createElement("ul");
      entries.forEach(function (entry) {
        list.appendChild(listItem(navLink(root, table.path, entry.anchor, entry.name, entry.description)));
      });
      section.appendChild(list);
    });
    return section;
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

    nav.schemas.forEach(function (schema) {
      var schemaNode = treeNode(navLink(root, schema.path, "", schema.name, schema.description));
      var tableList = document.createElement("ul");

      schema.tables.forEach(function (table) {
        var tableNode = treeNode(navLink(root, table.path, "", table.name, table.description));
        if (table.path === page.tablePath) {
          fillTableSections(tableNode, root, table);
          tableNode.open = true;
        } else {
          tableNode.addEventListener("toggle", function fill() {
            tableNode.removeEventListener("toggle", fill);
            fillTableSections(tableNode, root, table);
          });
        }
        tableList.appendChild(listItem(tableNode));
      });

      schemaNode.appendChild(tableList);
      schemaNode.open = schema.path === page.schemaPath;
      schemaList.appendChild(listItem(schemaNode));
    });

    container.appendChild(schemaList);

    // Clicking a schema or table link navigates without toggling its node,
    // preventing the flash of the node collapsing on click.
    container.addEventListener("click", function (event) {
      var link = event.target.closest("summary > a");
      if (link) {
        event.preventDefault();
        window.location.href = link.href;
      }
    });
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
